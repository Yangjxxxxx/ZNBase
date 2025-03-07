  
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

import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import { RouterState, Link } from "react-router";
import _ from "lodash";

import "./nodeOverview.styl";

import {
  livenessNomenclature, LivenessStatus, NodesSummary, nodesSummarySelector, selectNodesSummaryValid,
} from "src/redux/nodes";
import { nodeIDAttr } from "src/util/constants";
import { AdminUIState } from "src/redux/state";
import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import { INodeStatus, MetricConstants, StatusMetrics } from  "src/util/proto";
import { Bytes, Percentage } from "src/util/format";
import { LongToMoment } from "src/util/convert";
import {
  SummaryBar, SummaryLabel, SummaryValue,
} from "src/views/shared/components/summaryBar";

interface NodeOverviewProps extends RouterState {
  node: INodeStatus;
  nodesSummary: NodesSummary;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  // True if current status results are still valid. Needed so that this
  // component refreshes status query when it becomes invalid.
  nodesSummaryValid: boolean;
}

/**
 * Renders the Node Overview page.
 */
class NodeOverview extends React.Component<NodeOverviewProps, {}> {
  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentWillReceiveProps(props: NodeOverviewProps) {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    props.refreshNodes();
    props.refreshLiveness();
  }

  render() {
    const { node, nodesSummary } = this.props;
    if (!node) {
      return (
        <div className="section">
          <h1>加载节点状态中..... </h1>
        </div>
      );
    }

    const liveness = nodesSummary.livenessStatusByNodeID[node.desc.node_id] || LivenessStatus.LIVE;
    const livenessString = livenessNomenclature(liveness);

    return (
      <div>
        <Helmet>
          <title>{`${nodesSummary.nodeDisplayNameByID[node.desc.node_id]} | 节点`}</title>
        </Helmet>
        <div className="section section--heading">
          <h2>{`节点 ${node.desc.node_id} / ${node.desc.address.address_field}`}</h2>
        </div>
        <section className="section l-columns">
          <div className="l-columns__left">
            <table className="table">
              <thead>
                <tr className="table__row table__row--header">
                  <th className="table__cell" />
                  <th className="table__cell">{`节点${node.desc.node_id}`}</th>
                  {
                    _.map(node.store_statuses, (ss) => {
                      const storeId = ss.desc.store_id;
                      return <th key={storeId} className="table__cell">{`存储${storeId}`}</th>;
                    })
                  }
                  <th className="table__cell table__cell--filler" />
                </tr>
              </thead>
              <tbody>
                <TableRow data={node}
                          title="Live Bytes"
                          valueFn={(metrics) => Bytes(metrics[MetricConstants.liveBytes])} />
                <TableRow data={node}
                          title="Key Bytes"
                          valueFn={(metrics) => Bytes(metrics[MetricConstants.keyBytes])} />
                <TableRow data={node}
                          title="Value Bytes"
                          valueFn={(metrics) => Bytes(metrics[MetricConstants.valBytes])} />
                <TableRow data={node}
                          title="Intent Bytes"
                          valueFn={(metrics) => Bytes(metrics[MetricConstants.intentBytes])} />
                <TableRow data={node}
                          title="Sys Bytes"
                          valueFn={(metrics) => Bytes(metrics[MetricConstants.sysBytes])} />
                <TableRow data={node}
                          title="GC Bytes Age"
                          valueFn={(metrics) => metrics[MetricConstants.gcBytesAge].toString()} />
                <TableRow data={node}
                          title="副 本 总 数"
                          valueFn={(metrics) => metrics[MetricConstants.replicas].toString()} />
                <TableRow data={node}
                          title="Raft Leaders"
                          valueFn={(metrics) => metrics[MetricConstants.raftLeaders].toString()} />
                <TableRow data={node}
                          title="Ranges 总数"
                          valueFn={(metrics) => metrics[MetricConstants.ranges]} />
                <TableRow data={node}
                          title="不可用"
                          valueFn={(metrics) => Percentage(metrics[MetricConstants.unavailableRanges], metrics[MetricConstants.ranges])} />
                <TableRow data={node}
                          title="复制中"
                          valueFn={(metrics) => Percentage(metrics[MetricConstants.underReplicatedRanges], metrics[MetricConstants.ranges])} />
                <TableRow data={node}
                          title="已使用存储"
                          valueFn={(metrics) => Bytes(metrics[MetricConstants.usedCapacity])} />
                <TableRow data={node}
                          title="可 用 存 储"
                          valueFn={(metrics) => Bytes(metrics[MetricConstants.availableCapacity])} />
                <TableRow data={node}
                          title="存 储 总 量"
                          valueFn={(metrics) => Bytes(metrics[MetricConstants.capacity])} />
              </tbody>
            </table>
          </div>
          <div className="l-columns__right">
            <SummaryBar>
              <SummaryLabel>节点汇总</SummaryLabel>
              <SummaryValue
                title="状态"
                value={livenessString}
                classModifier={livenessString}
              />
              <SummaryValue title="最新更新" value={LongToMoment(node.updated_at).fromNow()} />
              <SummaryValue title="版本" value={node.build_info.tag} />
              <SummaryValue
                title="日志"
                value={<Link to={`/node/${node.desc.node_id}/logs`}>查看日志</Link>}
                classModifier="link"
              />
            </SummaryBar>
          </div>
        </section>
      </div>
    );
  }
}

/**
 * TableRow is a small stateless component that renders a single row in the node
 * overview table. Each row renders a store metrics value, comparing the value
 * across the different stores on the node (along with a total value for the
 * node itself).
 */
function TableRow(props: { data: INodeStatus, title: string, valueFn: (s: StatusMetrics) => React.ReactNode }) {
  return <tr className="table__row table__row--body">
    <td className="table__cell">{ props.title }</td>
    <td className="table__cell">{ props.valueFn(props.data.metrics) }</td>
    {
      _.map(props.data.store_statuses, (ss) => {
        return <td key={ss.desc.store_id} className="table__cell">{ props.valueFn(ss.metrics) }</td>;
      })
    }
    <td className="table__cell table__cell--filler" />
  </tr>;
}

export const currentNode = createSelector(
  (state: AdminUIState, _props: RouterState): INodeStatus[] => state.cachedData.nodes.data,
  (_state: AdminUIState, props: RouterState): number => parseInt(props.params[nodeIDAttr], 10),
  (nodes, id) => {
    if (!nodes || !id) {
      return undefined;
    }
    return _.find(nodes, (ns) => ns.desc.node_id === id);
  });

export default connect(
  (state: AdminUIState, ownProps: RouterState) => {
    return {
      node: currentNode(state, ownProps),
      nodesSummary: nodesSummarySelector(state),
      nodesSummaryValid: selectNodesSummaryValid(state),
    };
  },
  {
    refreshNodes,
    refreshLiveness,
  },
)(NodeOverview);
