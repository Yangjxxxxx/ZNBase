package optbuilder

import "github.com/znbasedb/znbase/pkg/sql/sem/tree"

// lockingSpec maintains a collection of FOR [KEY] UPDATE/SHARE items that apply
// to a given scope. Locking clauses can be applied to the lockingSpec as they
// come into scope in the AST. The lockingSpec can then be consolidated down to
// a single row-level locking specification for different tables to determine
// how scans over those tables should perform row-level locking, if at all.
// LockingSpec维护适用于给定作用域的FOR [KEY]更新/共享项的集合。 当锁定子句进入AST中的作用域时，
// 可以将其应用于lockingSpec。 然后可以将lockingSpec合并为不同表的单个行级锁定规范， 以确定扫描
// 这些表应该如何执行行级锁定(如果有的话)。

// A SELECT statement may contain zero, one, or more than one row-level locking
// clause. Each of these clauses consist of two different properties.
// 一个SELECT语句可以包含零个，一个或多个行级锁定子句。 这些子句中的每一个都包含两个不同的属性。

// The first property is locking strength (see tree.LockingStrength). Locking
// strength represents the degree of protection that a row-level lock provides.
// The stronger the lock, the more protection it provides for the lock holder
// but the more restrictive it is to concurrent transactions attempting to
// access the same row. In order from weakest to strongest, the lock strength
// variants are:
// 第一个属性是锁定强度（请参见tree.LockingStrength）。 锁定强度表示行级锁定提供的保护程度。
// 锁越强，它为锁持有者提供的保护就越多，但对尝试访问同一行的并发事务的限制就越大。按照从最弱到
// 最强的顺序，锁定强度的变化是：
//   FOR KEY SHARE
//   FOR SHARE
//   FOR NO KEY UPDATE
//   FOR UPDATE
//
// The second property is the locking wait policy (see tree.LockingWaitPolicy).
// A locking wait policy represents the policy a table scan uses to interact
// with row-level locks held by other transactions. Unlike locking strength,
// locking wait policy is optional to specify in a locking clause. If not
// specified, the policy defaults to blocking and waiting for locks to become
// available. The non-standard policies instruct scans to take other approaches
// to handling locks held by other transactions. These non-standard policies
// are:
// 第二个属性是锁定等待策略（请参阅tree.LockingWaitPolicy）。 锁定等待策略表示表扫描用于与
// 其他事务持有的行级锁进行交互的策略。与锁定强度不同， 锁定等待策略是可选的，可以在锁定子句中
// 指定。如果未指定，则该策略默认为阻止并等待锁变为可用。非标准策略指示扫描采取其他方法来处理其
// 他事务持有的锁。 这些非标准策略是：
//   SKIP LOCKED
//   NOWAIT
//
// In addition to these two properties, locking clauses can contain an optional
// list of target relations. When provided, the locking clause applies only to
// those relations in the target list. When not provided, the locking clause
// applies to all relations in the current scope.
// 除了这两个属性外，locking子句还可以包含目标关系的可选列表。提供时，锁定子句仅适用于目标列表
// 中的那些关系。如果未提供，则子句适用于当前范围内的所有关系。

// Put together, a complex locking spec might look like:
// 放在一起，复杂的锁定规范可能看起来像：
//   SELECT ... FROM ... FOR SHARE NOWAIT FOR UPDATE OF t1, t2
//
// which would be represented as:
// 可以表示为：
//   [ {ForShare, LockWaitError, []}, {ForUpdate, LockWaitBlock, [t1, t2]} ]
//
type lockingSpec []*tree.LockingItem

// noRowLocking indicates that no row-level locking has been specified.
// noRowLocking表示未指定行级锁定。
var noRowLocking lockingSpec

// isSet returns whether the spec contains any row-level locking modes.
// isSet返回规范是否包含任何行级锁定模式。
func (lm lockingSpec) isSet() bool {
	return len(lm) != 0
}

// get returns the first row-level locking mode in the spec. If the spec was the
// outcome of filter operation, this will be the only locking mode in the spec.
// get返回规范中的第一个行级锁定模式。 如果规格是过滤器操作的结果，则它将是规格中唯一的锁定模式。
func (lm lockingSpec) get() *tree.LockingItem {
	if lm.isSet() {
		return lm[0]
	}
	return nil
}

// apply merges the locking clause into the current locking spec. The effect of
// applying new locking clauses to an existing spec is always to strengthen the
// locking approaches it represents, either through increasing locking strength
// or using more aggressive wait policies.
// apply将锁定子句合并到当前锁定规范中。将新的锁定子句应用于现有规范的效果始终是通过提高锁定强度
// 或使用更具攻击性的等待策略来增强其代表的锁定方法。
func (lm *lockingSpec) apply(locking tree.LockingClause) {
	// TODO(nvanbenschoten): If we wanted to eagerly prune superfluous locking
	// items so that they don't need to get merged away in each call to filter,
	// this would be the place to do it. We don't expect to see multiple FOR
	// UPDATE clauses very often, so it's probably not worth it.
	// TODO（nvanbenschoten）：如果我们想急切地删除多余的锁定项，这样它们就不需要在每次筛选调
	// 用中被合并掉，那么这里就是这样做的地方。我们不希望经常看到多个FOR UPDATE子句，所以这可能
	// 不值得。
	//
	if len(*lm) == 0 {
		// NB: avoid allocation, but also prevent future mutation of AST.
		// 注意：避免分配，但也可以防止将来的AST突变
		l := len(locking)
		*lm = lockingSpec(locking[:l:l])
		return
	}
	*lm = append(*lm, locking...)
}

// filter returns the desired row-level locking mode for the specifies table as
// a new consolidated lockingSpec. If no matching locking mode is found then the
// resulting spec will remain un-set. If a matching locking mode for the table
// is found then the resulting spec will contain exclusively that locking mode
// and will no longer be restricted to specific target relations.
// filter将指定表的所需级锁定模式返回为新的合并的LockingSpec。 如果未找到匹配的锁定模式，则结
// 果规格将保持不变。如果找到该表的匹配锁定模式，那么生成的规范将仅包含该锁定模式，并且不再局限于
// 特定的目标关系。
func (lm lockingSpec) filter(alias tree.Name) lockingSpec {
	var ret lockingSpec
	var copied bool
	updateRet := func(li *tree.LockingItem, len1 []*tree.LockingItem) {
		if ret == nil && len(li.Targets) == 0 {
			// Fast-path. We don't want the resulting spec to include targets,
			// so we only allow this if the item we want to copy has none.
			// Fast-path（快速路径）。我们不希望最终的规范包含目标，因此仅当要复制的项目没有任何目标时，
			// 才允许这样做。
			ret = len1
			return
		}
		if !copied {
			retCpy := make(lockingSpec, 1)
			retCpy[0] = new(tree.LockingItem)
			if len(ret) == 1 {
				*retCpy[0] = *ret[0]
			}
			ret = retCpy
			copied = true
		}
		// From https://www.postgresql.org/docs/12/sql-select.html#SQL-FOR-UPDATE-SHARE
		// > If the same table is mentioned (or implicitly affected) by more
		// > than one locking clause, then it is processed as if it was only
		// > specified by the strongest one.
		// 如果同一个表被多个锁子句提及（或隐式影响），那么它将被当作只由最强的一个指定。
		// 来自tps://www.postgresql.org/docs/12/sql select.html #sql-FOR-UPDATE-SHARE
		ret[0].Strength = ret[0].Strength.Max(li.Strength)
		// > Similarly, a table is processed as NOWAIT if that is specified in
		// > any of the clauses affecting it. Otherwise, it is processed as SKIP
		// > LOCKED if that is specified in any of the clauses affecting it.
		// 类似地，如果在影响表的任何子句中指定了NOWAIT，则将其处理为NOWAIT。否则，如果在影响
		// 它的任何子句中指定了SKIP LOCKED，则将其处理为SKIP LOCKED。
		ret[0].WaitPolicy = ret[0].WaitPolicy.Max(li.WaitPolicy)
	}

	for i, li := range lm {
		len1 := lm[i : i+1 : i+1]
		if len(li.Targets) == 0 {
			// If no targets are specified, the clause affects all tables.
			// 如果没有指定目标，则子句将影响所有表。
			updateRet(li, len1)
		} else {
			// If targets are specified, the clause affects only those tables.
			// 如果指定了目标，则子句只影响那些被指定了目标的表。
			for _, target := range li.Targets {
				if target.TableName == alias {
					updateRet(li, len1)
					break
				}
			}
		}
	}
	return ret
}

// withoutTargets returns a new lockingSpec with all locking clauses that apply
// only to a subset of tables removed.
// withoutTargets返回一个新的lockingSpec，其中所有锁定子句仅适用于已删除表的子集。
func (lm lockingSpec) withoutTargets() lockingSpec {
	return lm.filter("")
}

// ignoreLockingForCTE is a placeholder for the following comment:
// ignoreLockingForCTE是以下注释的占位符：
//
// We intentionally do not propate any row-level locking information from the
// current scope to the CTE. This mirrors Postgres' behavior. It also avoids a
// number of awkward questions like how row-level locking would interact with
// mutating commong table expressions.
// 我们有意不将任何行级锁定信息从当前作用域提供给CTE。 这反映了Postgres的行为。它还避免了许多
// 尴尬的问题，例如行级锁定如何与更改commong表表达式交互。
//
// From https://www.postgresql.org/docs/12/sql-select.html#SQL-FOR-UPDATE-SHARE
// > these clauses do not apply to WITH queries referenced by the primary query.
// > If you want row locking to occur within a WITH query, specify a locking
// > clause within the WITH query.
// ignoreLockingForCTE来自https://www.postgresql.org/docs/12/sql-select.html#SQL-FOR-UPDATE-SHARE
// 这些子句不适用于主查询引用的WITH查询。
// 如果希望在WITH查询中发生行锁定，请在WITH查询中指定一个锁定子句。
func (lm lockingSpec) ignoreLockingForCTE() {}
