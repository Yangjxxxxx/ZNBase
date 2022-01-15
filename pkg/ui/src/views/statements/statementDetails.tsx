  
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import d3 from "d3";
import _ from "lodash";
import React, { ReactNode } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, RouterState } from "react-router";
import { createSelector } from "reselect";

import Loading from "src/views/shared/components/loading";
import { refreshStatements } from "src/redux/apiReducers";
import { nodeDisplayNameByIDSelector } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { NumericStat, stdDev, combineStatementStats, flattenStatementStats, StatementStatistics, ExecutionStatistics } from "src/util/appStats";
import { statementAttr, appAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import { Duration } from "src/util/format";
import { intersperse } from "src/util/intersperse";
import { Pick } from "src/util/pick";
import { PlanView } from "src/views/statements/planView";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SqlBox } from "src/views/shared/components/sql/box";
import { SummaryBar, SummaryHeadlineStat } from "src/views/shared/components/summaryBar";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

import { countBreakdown, rowsBreakdown, latencyBreakdown, approximify } from "./barCharts";
import { AggregateStatistics, StatementsSortedTable, makeNodesColumns } from "./statementsTable";

interface Fraction {
  numerator: number;
  denominator: number;
}

interface SingleStatementStatistics {
  statement: string;
  app: string[];
  distSQL: Fraction;
  opt: Fraction;
  failed: Fraction;
  node_id: number[];
  stats: StatementStatistics;
  byNode: AggregateStatistics[];
}

function AppLink(props: { app: string }) {
  if (!props.app) {
    return <span className="app-name app-name__unset">返回查看语句</span>;
  }

  return (
    <Link className="app-name" to={ `/statements/${encodeURIComponent(props.app)}` }>
      返回查看语句
    </Link>
  );
}

interface StatementDetailsOwnProps {
  statement: SingleStatementStatistics;
  statementsError: Error | null;
  nodeNames: { [nodeId: string]: string };
  refreshStatements: typeof refreshStatements;
}

type StatementDetailsProps = StatementDetailsOwnProps & RouterState;

interface StatementDetailsState {
  sortSetting: SortSetting;
}

interface NumericStatRow {
  name: string;
  value: NumericStat;
  bar?: () => ReactNode;
  summary?: boolean;
}

interface NumericStatTableProps {
  title?: string;
  description?: string;
  measure: string;
  rows: NumericStatRow[];
  count: number;
  format?: (v: number) => string;
}

class NumericStatTable extends React.Component<NumericStatTableProps> {
  static defaultProps = {
    format: (v: number) => `${v}`,
  };

  render() {
    const tooltip = !this.props.description ? null : (
        <div className="numeric-stats-table__tooltip">
          <ToolTipWrapper text={this.props.description}>
            <div className="numeric-stats-table__tooltip-hover-area">
              <div className="numeric-stats-table__info-icon">i</div>
            </div>
          </ToolTipWrapper>
        </div>
      );

    return (
      <table className="numeric-stats-table">
        <thead>
          <tr className="numeric-stats-table__row--header">
            <th className="numeric-stats-table__cell">
              { this.props.title }
              { tooltip }
            </th>
            <th className="numeric-stats-table__cell">平均{this.props.measure}</th>
            <th className="numeric-stats-table__cell">标准差</th>
          </tr>
        </thead>
        <tbody style={{ textAlign: "right" }}>
          {
            this.props.rows.map((row: NumericStatRow) => {
              const classNames = "numeric-stats-table__row--body" +
                (row.summary ? " numeric-stats-table__row--summary" : "");
              return (
                <tr className={classNames}>
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>{ row.name }</th>
                  <td className="numeric-stats-table__cell">{ row.bar ? row.bar() : null }</td>
                  <td className="numeric-stats-table__cell">{ this.props.format(stdDev(row.value, this.props.count)) }</td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    );
  }
}

class StatementDetails extends React.Component<StatementDetailsProps, StatementDetailsState> {

  constructor(props: StatementDetailsProps) {
    super(props);
    this.state = {
      sortSetting: {
        sortKey: 1,
        ascending: false,
      },
    };
  }

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  }

  componentWillMount() {
    this.props.refreshStatements();
  }

  componentWillReceiveProps() {
    this.props.refreshStatements();
  }

  render() {
    return (
      <div>
        <Helmet>
          <title>
            { "细节 | " + (this.props.params[appAttr] ? this.props.params[appAttr] + " 应用 | " : "") + "语句" }
          </title>
        </Helmet>
        <section className="section"><h1>语句执行详情</h1></section>
        <section className="section section--container">
          <Loading
            loading={_.isNil(this.props.statement)}
            error={this.props.statementsError}
            render={this.renderContent}
          />
        </section>
      </div>
    );
  }

  renderContent = () => {
    if (!this.props.statement) {
      return null;
    }

    const { stats, statement, app, distSQL, opt, failed } = this.props.statement;

    if (!stats) {
      const sourceApp = this.props.params[appAttr];
      const listUrl = "/statements" + (sourceApp ? "/" + sourceApp : "");

      return (
        <React.Fragment>
          <section className="section">
            <SqlBox value={ statement } />
          </section>
          <section className="section">
            <h3>无法找到语句</h3>
            {/* There are no execution statistics for this statement.{" "} */}
            此语句没有执行统计信息。{" "}
            <Link className="back-link" to={ listUrl }>
              回到语句
            </Link>
          </section>
        </React.Fragment>
      );
    }

    const count = FixLong(stats.count).toInt();
    const firstAttemptCount = FixLong(stats.first_attempt_count).toInt();

    const { firstAttemptsBarChart, retriesBarChart, maxRetriesBarChart, totalCountBarChart } = countBreakdown(this.props.statement);
    const { rowsBarChart } = rowsBreakdown(this.props.statement);
    const { parseBarChart, planBarChart, runBarChart, overheadBarChart, overallBarChart } = latencyBreakdown(this.props.statement);

    const statsByNode = this.props.statement.byNode;
    const logicalPlan = stats.sensitive_info && stats.sensitive_info.most_recent_plan_description;

    return (
      <div className="content l-columns">
        <div className="l-columns__left">
          <section className="section section--heading">
            <SqlBox value={ statement } />
          </section>
          <section className="section">
            <PlanView
              title="逻辑计划"
              plan={logicalPlan} />
          </section>
          <section className="section">
            <NumericStatTable
              title="阶段"
              description="按阶段细分此语句的执行延迟。"
              measure="延迟"
              count={ count }
              format={ (v: number) => Duration(v * 1e9) }
              rows={[
                { name: "解析", value: stats.parse_lat, bar: parseBarChart },
                { name: "计划", value: stats.plan_lat, bar: planBarChart },
                { name: "执行", value: stats.run_lat, bar: runBarChart },
                { name: "其他", value: stats.overhead_lat, bar: overheadBarChart },
                { name: "合计", summary: true, value: stats.service_lat, bar: overallBarChart },
              ]}
            />
          </section>
          <section className="section">
            <StatementsSortedTable
              className="statements-table"
              data={statsByNode}
              columns={makeNodesColumns(statsByNode, this.props.nodeNames)}
              sortSetting={this.state.sortSetting}
              onChangeSortSetting={this.changeSortSetting}
            />
          </section>
          <section className="section">
            <table className="numeric-stats-table">
              <thead>
                <tr className="numeric-stats-table__row--header">
                  <th className="numeric-stats-table__cell" colSpan={ 3 }>
                   执行次数
                    <div className="numeric-stats-table__tooltip">
                      {/* <ToolTipWrapper text="The number of times this statement has been executed."> */}
                      <ToolTipWrapper text="执行此语句的次数。">
                        <div className="numeric-stats-table__tooltip-hover-area">
                          <div className="numeric-stats-table__info-icon">i</div>
                        </div>
                      </ToolTipWrapper>
                    </div>
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr className="numeric-stats-table__row--body">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>首次成功次数</th>
                  <td className="numeric-stats-table__cell">{ firstAttemptsBarChart() }</td>
                </tr>
                <tr className="numeric-stats-table__row--body">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>重试次数</th>
                  <td className="numeric-stats-table__cell">{ retriesBarChart() }</td>
                </tr>
                <tr className="numeric-stats-table__row--body">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>最大重试次数</th>
                  <td className="numeric-stats-table__cell">{ maxRetriesBarChart() }</td>
                </tr>
                <tr className="numeric-stats-table__row--body numeric-stats-table__row--summary">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>合计</th>
                  <td className="numeric-stats-table__cell">{ totalCountBarChart() }</td>
                </tr>
              </tbody>
            </table>
          </section>
          <section className="section">
            <NumericStatTable
              measure="行"
              count={ count }
              format={ (v: number) => "" + (Math.round(v * 100) / 100) }
              rows={[
                { name: "影响行数", value: stats.num_rows, bar: rowsBarChart },
              ]}
            />
          </section>
        </div>
        <div className="l-columns__right">
          <SummaryBar>
            <SummaryHeadlineStat
              title="所有时间"
              // tooltip="Cumulative time spent servicing this statement."
              tooltip="维护此语句所花费的累计时间。"
              value={ count * stats.service_lat.mean }
              format={ v => Duration(v * 1e9) } />
            <SummaryHeadlineStat
              title="执行次数"
              // tooltip="Number of times this statement has executed."
              tooltip="执行此语句的次数。"
              value={ count }
              format={ approximify } />
            <SummaryHeadlineStat
              title="首次成功几率"
              // tooltip="Portion of executions free of retries."
              tooltip="部分执行没有重试。"
              value={ firstAttemptCount / count }
              format={ d3.format("%") } />
            <SummaryHeadlineStat
              title="平均服务延迟"
              // tooltip="Latency to parse, plan, and execute the statement."
              tooltip="分析、计划和执行语句的延迟。"
              value={ stats.service_lat.mean }
              format={ v => Duration(v * 1e9) } />
            <SummaryHeadlineStat
              title="平均行数"
              // tooltip="The average number of rows returned or affected."
              tooltip="返回或受影响的平均行数。"
              value={ stats.num_rows.mean }
              format={ approximify } />
          </SummaryBar>
          <table className="numeric-stats-table">
            <tbody>
              <tr className="numeric-stats-table__row--body">
                <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>应用</th>
                <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>
                  {/* { intersperse<ReactNode>(app.map(a => <AppLink app={ a } key={ a } />), ", ") } */}
                  { intersperse<ReactNode>(app.map(a => <AppLink app={ a } key={ a } />), ", ") }
                </td>
              </tr>
              <tr className="numeric-stats-table__row--body">
                <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>是否分布式执行</th>
                <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ renderBools(distSQL) }</td>
              </tr>
              <tr className="numeric-stats-table__row--body">
                <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>是否使用基于成本的优化器</th>
                <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ renderBools(opt) }</td>
              </tr>
              <tr className="numeric-stats-table__row--body">
                <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>是否失败</th>
                <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ renderBools(failed) }</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    );
  }
}

function renderBools(fraction: Fraction) {
  if (Number.isNaN(fraction.numerator)) {
    return "(unknown)";
  }
  if (fraction.numerator === 0) {
    return "No";
  }
  if (fraction.numerator === fraction.denominator) {
    return "Yes";
  }
  return approximify(fraction.numerator) + " of " + approximify(fraction.denominator);
}

type StatementsState = Pick<AdminUIState, "cachedData", "statements">;

function coalesceNodeStats(stats: ExecutionStatistics[]): AggregateStatistics[] {
  const byNode: { [nodeId: string]: StatementStatistics[] } = {};

  stats.forEach(stmt => {
    const nodeStats = (byNode[stmt.node_id] = byNode[stmt.node_id] || []);
    nodeStats.push(stmt.stats);
  });

  return Object.keys(byNode).map(nodeId => ({
      label: nodeId,
      stats: combineStatementStats(byNode[nodeId]),
  }));
}

function fractionMatching(stats: ExecutionStatistics[], predicate: (stmt: ExecutionStatistics) => boolean): Fraction {
  let numerator = 0;
  let denominator = 0;

  stats.forEach(stmt => {
    const count = FixLong(stmt.stats.first_attempt_count).toInt();
    denominator += count;
    if (predicate(stmt)) {
      numerator += count;
    }
  });

  return { numerator, denominator };
}

export const selectStatement = createSelector(
  (state: StatementsState) => state.cachedData.statements.data && state.cachedData.statements.data.statements,
  (_state: StatementsState, props: { params: { [key: string]: string } }) => props,
  (statements, props) => {
    if (!statements) {
      return null;
    }

    const statement = props.params[statementAttr];
    let app = props.params[appAttr];
    let predicate = (stmt: ExecutionStatistics) => stmt.statement === statement;

    if (app) {
        if (app === "(unset)") {
            app = "";
        }
        predicate = (stmt: ExecutionStatistics) => stmt.statement === statement && stmt.app === app;
    }

    const flattened = flattenStatementStats(statements);
    const results = _.filter(flattened, predicate);

    return {
      statement,
      stats: combineStatementStats(results.map(s => s.stats)),
      byNode: coalesceNodeStats(results),
      app: _.uniq(results.map(s => s.app)),
      distSQL: fractionMatching(results, s => s.distSQL),
      opt: fractionMatching(results, s => s.opt),
      failed: fractionMatching(results, s => s.failed),
      node_id: _.uniq(results.map(s => s.node_id)),
    };
  },
);

// tslint:disable-next-line:variable-name
const StatementDetailsConnected = connect(
  (state: AdminUIState, props: RouterState) => ({
    statement: selectStatement(state, props),
    statementsError: state.cachedData.statements.lastError,
    nodeNames: nodeDisplayNameByIDSelector(state),
  }),
  {
    refreshStatements,
  },
)(StatementDetails);

export default StatementDetailsConnected;
