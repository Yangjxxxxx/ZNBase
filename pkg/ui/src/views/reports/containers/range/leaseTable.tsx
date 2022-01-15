  
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
import React from "react";

import * as protos from "src/js/protos";
import Lease from "src/views/reports/containers/range/lease";
import Print from "src/views/reports/containers/range/print";
import RangeInfo from "src/views/reports/containers/range/rangeInfo";

interface LeaseTableProps {
  info: protos.znbase.server.serverpb.IRangeInfo;
}

export default class LeaseTable extends React.Component<LeaseTableProps, {}> {
  renderLeaseCell(value: string, title: string = "") {
    if (_.isEmpty(title)) {
      return <td className="lease-table__cell" title={value}>{value}</td>;
    }
    return <td className="lease-table__cell" title={title}>{value}</td>;
  }

  renderLeaseTimestampCell(timestamp: protos.znbase.util.hlc.ITimestamp) {
    if (_.isNil(timestamp)) {
      return this.renderLeaseCell("<no value>");
    }

    const value = Print.Timestamp(timestamp);
    return this.renderLeaseCell(value, `${value}\n${timestamp.wall_time.toString()}`);
  }

  render() {
    const { info } = this.props;
    // TODO(bram): Maybe search for the latest lease record instead of just trusting the
    // leader?
    const rangeID = info.state.state.desc.range_id;
    const header = (
      <h2>
        Lease 历史 (from {Print.ReplicaID(rangeID, RangeInfo.GetLocalReplica(info))})
      </h2>
    );
    if (_.isEmpty(info.lease_history)) {
      return (
        <div>
          {header}
          <h3>这个Range没有任何lease历史</h3>
        </div>
      );
    }

    const isEpoch = Lease.IsEpoch(_.head(info.lease_history));
    const leaseHistory = _.reverse(info.lease_history);
    return (
      <div>
        {header}
        <table className="lease-table">
          <tbody>
            <tr className="lease-table__row lease-table__row--header">
              <th className="lease-table__cell lease-table__cell--header">副本</th>
              {
                isEpoch ? <th className="lease-table__cell lease-table__cell--header">Epoch</th> : null
              }
              <th className="lease-table__cell lease-table__cell--header">提出时间</th>
              <th className="lease-table__cell lease-table__cell--header">提议的增量</th>
              {
                !isEpoch ? <th className="lease-table__cell lease-table__cell--header">到期时间</th> : null
              }
              <th className="lease-table__cell lease-table__cell--header">启动</th>
              <th className="lease-table__cell lease-table__cell--header">启动时的增量</th>
            </tr>
            {
              _.map(leaseHistory, (lease, key) => {
                let prevProposedTimestamp: protos.znbase.util.hlc.ITimestamp = null;
                let prevStart: protos.znbase.util.hlc.ITimestamp = null;
                if (key < leaseHistory.length - 1) {
                  prevProposedTimestamp = leaseHistory[key + 1].proposed_ts;
                  prevStart = leaseHistory[key + 1].start;
                }
                return (
                  <tr key={key} className="lease-table__row">
                    {this.renderLeaseCell(Print.ReplicaID(rangeID, lease.replica))}
                    {isEpoch ? this.renderLeaseCell(`n${lease.replica.node_id}, ${lease.epoch.toString()}`) : null}
                    {this.renderLeaseTimestampCell(lease.proposed_ts)}
                    {this.renderLeaseCell(Print.TimestampDelta(lease.proposed_ts, prevProposedTimestamp))}
                    {!isEpoch ? this.renderLeaseTimestampCell(lease.expiration) : null}
                    {this.renderLeaseTimestampCell(lease.start)}
                    {this.renderLeaseCell(Print.TimestampDelta(lease.start, prevStart))}
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );
  }
}
