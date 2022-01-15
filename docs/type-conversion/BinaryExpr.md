## znbase隐式类型转换简介

znbase中将所有数据类型都作为表达式参与类型转换，它们都实现了Expr接口。
Expr接口的
```go
TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error)
```
方法执行类型的检查和转换工作。
简单的整形、浮点、字符串等常量类型都实现了Constant接口。Constant接口提供AvailableTypes()和DesirableTypes()方法，它们返回该Constant类型可以转换成的类型；而ResolveAsType(*SemaContext, types.T) (Datum, error)方法执行类型转换。Constantl类型TypeCheck方法正是先调用AvailableTypes()获取一个类型列表，再将其中的类型和desired对比。若找到了desired类型，则调用ResolveAsType执行转换。Constantl接口有NumVal和StrVal两个实现，前者是数值类型，后者是字符串类型。

含加减乘除运算符的计算表达式属于BinaryExpr，它是OperatorExpr接口的一个实现：
```go
type BinaryExpr struct {
	Operator    BinaryOperator
	Left, Right Expr

	typeAnnotation
	fn *BinOp
}
```
包含运算符、左右的子表达式和本表达式的结果类型等属性。
BinaryExpr的TypeCheck方法先取出Operator的所有重载，再根据左右表达式将其逐步筛去，直到剩下最合适的重载。其中比较关键的筛选步骤是，先对左表达式TypeCheck，把所有左端不符合这个类型的重载删去，再对右表达式执行这一操作。而在后续步骤中，存在一个步骤将子表达式的第一个AvailableTypes当做最合适的类型进行处理。执行筛选的函数是TypeCheckOverloadedExprs。该函数也可能用于其他有不止两个参数的表达式。

## 目前的尝试

根据需求，int, float和decimal类型都可以互相转换。所以调整了NumVal的AvailableTypes所检索的数组，允许高精度类型转换为低精度类型，加入了string，并且将高精度类型排在前面，使得在处理operator表达式时默认向高精度转换再进行计算。同理也调整了StrVal的AvailableTypes返回的顺序。

为了实现高精度数值类型转换为低精度(如decimal转int)，修改了NumVal的ResolveAsType函数，先统一转换成decimal类型再通过四舍五入等过程转换为目标类型。为了实现数值类型转换为字符串，在NumVal的ResolveAsType函数中加入了case types.String。

对于例如
```sql
INSERT INTO tint SELECT s FROM tstring WHERE s == '2.5';
```
一样的语句，我们无法在语法解析阶段判定select的结果是否有小数部分，而如果直接将查询子语句CAST成int类型，查询结果带有小数时会报错。一种解决方法是将可能发生这种情况的表达式套上两层CAST，先统一转换成decimal再转换成低精度类型；一种是修改实际执行时的函数ParseDInt(s string) (*DInt, error)，在其中先调用strconv.ParseFloat将字符串转换为浮点，再取整。这里选用了后者。

编写ChangeToCastExpr(expr Expr, desired types.T) CastExpr 函数，将一个Expr外面包上一层已经typecheck完毕的CastExpr，转换成指定类型。对于加减乘除等BinaryExpr，原本的筛选重载运算符的函数TypeCheckOverloadedExprs先将输入参数分为const和resolvable两部分，两部分的处理方式不同，但基本思路是对每个参数分别执行TypeCheck，每TypeCheck一个参数就用它对重载运算符们进行一次筛选。

修改后的ToCastChangeToCastExpr函数先把所有参数expr都TypeCheck，选出其中优先级最高的类型，调用ChangeToCastExpr把所有参数expr包装成CastExpr，转换成最高优先级类型，再作为resolvable的参数执行后续的筛选步骤。

类型的优先级暂定为：
```go
PriorityArray = []T{
		String,
		Bytes,
		Bool,
		BitArray,
		Int,
		Float,
		Decimal,
		Date,
		Time,
		Timestamp,
		TimestampTZ,
		Interval,
		UUID,
		INet,
		JSON,
		Oid,
	}
```
下标越大优先级越高。目前实验的类型较少，这个优先级应该还存在不少问题。

对于Bool和date等类型转换成其他类型，可以为对应的Datum类型DBool、DDate等实现Constant接口，设定其AvailableTypes，并在ResolveAsType方法里具体实现向其他类型的转换。最后在TypeCheck函数中调用TypeCheckConstant。
对于其他类型转换成上述类型，需要将目标类型添加到源类型的AvailableTypes返回数组中，并在ResolveAsType里实现。特别地对于string源类型，还需要修改ParseDBool、ParseDDate等函数。
目前实现了bool。

目前可以实现的用例举例：
```sql
create table ti (i int);
create table ts (s string);
create table td (d decimal);
create table tb (b bool);

(insert some data)

insert into ti values(4.2);
insert into ti values('4.2');
insert into ti values(true);
insert into ts values(1);
insert into ts values(true); //结果是'1'
insert into ts values(4.2);
insert into tb values(1.2);
insert into tb values('-0.5');
insert into tb values('false');
select 1 + '1.5';
select 1 + (select s from ts where s = '4.2');
select 1 + true;
select true + 1.5;
select true + '1.5';
insert into ts select true + '1.5';
select ((select s from ts where s = '4.2') + (select i from ti where i = 3)) * (select b from tb where b = true);
insert into tb select (select s from ts where s = '4.2') + (select i from ti where i = 3);

prepare a as select 3 + case(4) when 4 then $1 else 42 end;
execute a ('2.2');  //5.2

prepare f as select ($1 + 2) + ($2 + '2.5');
execute f ('2.5', 2.5); //9.5

insert into ti values(date '1840-2-14');


```