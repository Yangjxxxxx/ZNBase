  
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
import { RouterState } from "react-router";

import * as protos from "src/js/protos";
import {
  allocatorRangeRequestKey,
  rangeRequestKey,
  rangeLogRequestKey,
  refreshAllocatorRange,
  refreshRange,
  refreshRangeLog,
} from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { rangeIDAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import ConnectionsTable from "src/views/reports/containers/range/connectionsTable";
import RangeTable from "src/views/reports/containers/range/rangeTable";
import LogTable from "src/views/reports/containers/range/logTable";
import AllocatorOutput from "src/views/reports/containers/range/allocator";
import RangeInfo from "src/views/reports/containers/range/rangeInfo";
import LeaseTable from "src/views/reports/containers/range/leaseTable";

interface RangeOwnProps {
  range: CachedDataReducerState<protos.znbase.server.serverpb.RangeResponse>;
  allocator: CachedDataReducerState<protos.znbase.server.serverpb.AllocatorRangeResponse>;
  rangeLog: CachedDataReducerState<protos.znbase.server.serverpb.RangeLogResponse>;
  refreshRange: typeof refreshRange;
  refreshAllocatorRange: typeof refreshAllocatorRange;
  refreshRangeLog: typeof refreshRangeLog;
}

type RangeProps = RangeOwnProps & RouterState;

function ErrorPage(props: {
  rangeID: string;
  errorText: string;
  range?: CachedDataReducerState<protos.znbase.server.serverpb.RangeResponse>;
}) {
  return (
    <div className="section">
      <h1>r{props.rangeID}的Range报告</h1>
      <h2>{props.errorText}</h2>
      <ConnectionsTable range={props.range} />
    </div>
  );
}

function rangeRequestFromProps(props: RangeProps) {
  return new protos.znbase.server.serverpb.RangeRequest({
    range_id: Long.fromString(props.params[rangeIDAttr]),
  });
}

function allocatorRequestFromProps(props: RangeProps) {
  return new protos.znbase.server.serverpb.AllocatorRangeRequest({
    range_id: Long.fromString(props.params[rangeIDAttr]),
  });
}

function rangeLogRequestFromProps(props: RangeProps) {
  // TODO(bram): Remove this limit once #18159 is resolved.
  return new protos.znbase.server.serverpb.RangeLogRequest({
    range_id: Long.fromString(props.params[rangeIDAttr]),
    limit: -1,
  });
}

/**
 * Renders the Range Report page.
 */
class Range extends React.Component<RangeProps, {}> {
  refresh(props = this.props) {
    props.refreshRange(rangeRequestFromProps(props));
    props.refreshAllocatorRange(allocatorRequestFromProps(props));
    props.refreshRangeLog(rangeLogRequestFromProps(props));
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentWillReceiveProps(nextProps: RangeProps) {
    if (this.props.location !== nextProps.location) {
      this.refresh(nextProps);
    }
  }

  render() {
    const rangeID = this.props.params[rangeIDAttr];
    const { range } = this.props;

    // A bunch of quick error cases.
    if (!_.isNil(range) && !_.isNil(range.lastError)) {
      return (
        <ErrorPage
          rangeID={rangeID}
          errorText={`加载 range ${range.lastError}时出错`}
        />
      );
    }
    if (_.isNil(range) || _.isEmpty(range.data)) {
      return (
        <ErrorPage rangeID={rangeID} errorText={`加载集群状态...`} />
      );
    }
    const responseRangeID = FixLong(range.data.range_id);
    if (!responseRangeID.eq(rangeID)) {
      return (
        <ErrorPage rangeID={rangeID} errorText={`更新集群状态...`} />
      );
    }
    if (responseRangeID.isNegative() || responseRangeID.isZero()) {
      return (
        <ErrorPage
          rangeID={rangeID}
          errorText={`Range ID 必须是一个非零的正整数"${rangeID}"`}
        />
      );
    }

    // Did we get any responses?
    if (!_.some(range.data.responses_by_node_id, resp => resp.infos.length > 0)) {
      return (
        <ErrorPage
          rangeID={rangeID}
          errorText={`没有结果, 可能 r${this.props.params[rangeIDAttr]} 并不存在`}
          range={range}
        />
      );
    }

    // Collect all the infos and sort them, putting the leader (or the replica
    // with the highest term, first.
    const infos = _.orderBy(
      _.flatMap(range.data.responses_by_node_id, resp => {
        if (resp.response && _.isEmpty(resp.error_message)) {
          return resp.infos;
        }
        return [];
      }),
      [
        info => RangeInfo.IsLeader(info),
        info => FixLong(info.raft_state.applied).toNumber(),
        info => FixLong(info.raft_state.hard_state.term).toNumber(),
        info => {
          const localReplica = RangeInfo.GetLocalReplica(info);
          return _.isNil(localReplica) ? 0 : localReplica.replica_id;
        },
      ],
      ["desc", "desc", "desc", "asc"],
    );

    // Gather all replica IDs.
    const replicas = _.chain(infos)
      .flatMap(info => info.state.state.desc.replicas)
      .sortBy(rep => rep.replica_id)
      .sortedUniqBy(rep => rep.replica_id)
      .value();

    return (
      <div className="section">
        <Helmet>
          <title>{ `r${responseRangeID.toString()} Range | 调试` }</title>
        </Helmet>
        <h1> r{responseRangeID.toString()}的 Range 报告</h1>
        <RangeTable infos={infos} replicas={replicas} />
        <LeaseTable info={_.head(infos)} />
        <ConnectionsTable range={range} />
        <AllocatorOutput allocator={this.props.allocator} />
        <LogTable rangeID={responseRangeID} log={this.props.rangeLog} />
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState, props: RangeProps) {
  return {
    range: state.cachedData.range[rangeRequestKey(rangeRequestFromProps(props))],
    allocator: state.cachedData.allocatorRange[allocatorRangeRequestKey(allocatorRequestFromProps(props))],
    rangeLog: state.cachedData.rangeLog[rangeLogRequestKey(rangeLogRequestFromProps(props))],
  };
}

const actions = {
  refreshRange,
  refreshAllocatorRange,
  refreshRangeLog,
};

export default connect(mapStateToProps, actions)(Range);
