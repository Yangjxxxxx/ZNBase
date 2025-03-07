  
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

import _ from "lodash";
import Long from "long";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, RouterState } from "react-router";

import * as protos from "src/js/protos";
import { problemRangesRequestKey, refreshProblemRanges } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import ConnectionsTable from "src/views/reports/containers/problemRanges/connectionsTable";
import Loading from "src/views/shared/components/loading";

type NodeProblems$Properties = protos.znbase.server.serverpb.ProblemRangesResponse.INodeProblems;

interface ProblemRangesOwnProps {
  problemRanges: CachedDataReducerState<protos.znbase.server.serverpb.ProblemRangesResponse>;
  refreshProblemRanges: typeof refreshProblemRanges;
}

type ProblemRangesProps = ProblemRangesOwnProps & RouterState;

function isLoading(state: CachedDataReducerState<any>) {
  return _.isNil(state) || (_.isNil(state.data) && _.isNil(state.lastError));
}

function ProblemRangeList(props: {
  name: string,
  problems: NodeProblems$Properties[],
  extract: (p: NodeProblems$Properties) => Long[],
}) {
  const ids = _.chain(props.problems)
    .filter(problem => _.isEmpty(problem.error_message))
    .flatMap(problem => props.extract(problem))
    .map(id => FixLong(id))
    .sort((a, b) => a.compare(b))
    .map(id => id.toString())
    .sortedUniq()
    .value();
  if (_.isEmpty(ids)) {
    return null;
  }
  return (
    <div>
      <h2>{props.name}</h2>
      <div className="problems-list">
        {
          _.map(ids, id => {
            return (
              <Link key={id} className="problems-link" to={`reports/range/${id}`}>
                {id}
              </Link>
            );
          })
        }
      </div>
    </div>
  );
}

function problemRangeRequestFromProps(props: ProblemRangesProps) {
  return new protos.znbase.server.serverpb.ProblemRangesRequest({
    node_id: props.params[nodeIDAttr],
  });
}

/**
 * Renders the Problem Ranges page.
 *
 * The problem ranges endpoint returns a list of known ranges with issues on a
 * per node basis. This page aggregates those lists together and displays all
 * unique range IDs that have problems.
 */
class ProblemRanges extends React.Component<ProblemRangesProps, {}> {
  refresh(props = this.props) {
    props.refreshProblemRanges(problemRangeRequestFromProps(props));
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentWillReceiveProps(nextProps: ProblemRangesProps) {
    if (this.props.location !== nextProps.location) {
      this.refresh(nextProps);
    }
  }

  renderReportBody() {
    const { problemRanges } = this.props;
    if (isLoading(this.props.problemRanges)) {
      return null;
    }

    if (!_.isNil(problemRanges.lastError)) {
      if (_.isEmpty(this.props.params[nodeIDAttr])) {
        return (
          <div>
            <h2>加载群集的问题Ranges时出错</h2>
            {problemRanges.lastError.toString()}
          </div>
        );
      } else {
        return (
          <div>
            <h2>加载节点 n{this.props.params[nodeIDAttr]}的问题Ranges时出错</h2>
            {problemRanges.lastError.toString()}
          </div>
        );
      }
    }

    const { data } = problemRanges;

    const validIDs = _.keys(_.pickBy(data.problems_by_node_id, d => {
      return _.isEmpty(d.error_message);
    }));
    if (validIDs.length === 0) {
      if (_.isEmpty(this.props.params[nodeIDAttr])) {
        return <h2>没有节点返回任何结果</h2>;
      } else {
        return <h2>没有节点 n{this.props.params[nodeIDAttr]}的报告结果</h2>;
      }
    }

    let titleText: string; // = "Problem Ranges for ";
    if (validIDs.length === 1) {
      const singleNodeID = _.keys(data.problems_by_node_id)[0];
      titleText = `节点 n${singleNodeID}的问题 Ranges `;
    } else {
      titleText = "集群上的问题Ranges";
    }

    const problems = _.values(data.problems_by_node_id);
    return (
      <div>
        <h2>
          {titleText}
        </h2>
        <ProblemRangeList
          name="不可用的"
          problems={problems}
          extract={(problem) => problem.unavailable_range_ids}
        />
        <ProblemRangeList
          name="非 Raft Leader"
          problems={problems}
          extract={(problem) => problem.no_raft_leader_range_ids}
        />
        <ProblemRangeList
          name="无效的 Lease"
          problems={problems}
          extract={(problem) => problem.no_lease_range_ids}
        />
        <ProblemRangeList
          name="Raft Leader 但不是 Lease Holder"
          problems={problems}
          extract={(problem) => problem.raft_leader_not_lease_holder_range_ids}
        />
        <ProblemRangeList
          name="正在复制的（或速度较慢的）"
          problems={problems}
          extract={(problem) => problem.underreplicated_range_ids}
        />
        <ProblemRangeList
          name="过度复制的"
          problems={problems}
          extract={(problem) => problem.overreplicated_range_ids}
        />
        <ProblemRangeList
          name="停顿等于时钟的"
          problems={problems}
          extract={(problem) => problem.quiescent_equals_ticking_range_ids}
        />
      </div>
    );
  }

  render() {
    return (
      <div className="section">
        <Helmet>
          <title>有问题的 Ranges | 调试</title>
        </Helmet>
        <h1>有问题的 Ranges 报告</h1>
        <Loading
          loading={isLoading(this.props.problemRanges)}
          error={this.props.problemRanges && this.props.problemRanges.lastError}
          render={() => (
            <div>
              {this.renderReportBody()}
              <ConnectionsTable problemRanges={this.props.problemRanges} />
            </div>
          )}
        />
      </div>
    );
  }
}

export default connect(
  (state: AdminUIState, props: ProblemRangesProps) => {
    const nodeIDKey = problemRangesRequestKey(problemRangeRequestFromProps(props));
    return {
      problemRanges: state.cachedData.problemRanges[nodeIDKey],
    };
  },
  {
    refreshProblemRanges,
  },
)(ProblemRanges);
