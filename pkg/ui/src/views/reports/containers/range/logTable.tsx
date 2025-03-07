  
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
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { FixLong } from "src/util/fixLong";
import Print from "src/views/reports/containers/range/print";
import Loading from "src/views/shared/components/loading";
import { TimestampToMoment } from "src/util/convert";

interface LogTableProps {
  rangeID: Long;
  log: CachedDataReducerState<protos.znbase.server.serverpb.RangeLogResponse>;
}

function printLogEventType(
  eventType: protos.znbase.storage.RangeLogEventType,
) {
  switch (eventType) {
    case protos.znbase.storage.RangeLogEventType.add:
      return "Add";
    case protos.znbase.storage.RangeLogEventType.remove:
      return "Remove";
    case protos.znbase.storage.RangeLogEventType.split:
      return "Split";
    case protos.znbase.storage.RangeLogEventType.merge:
      return "Merge";
    default:
      return "Unknown";
  }
}

export default class LogTable extends React.Component<LogTableProps, {}> {
  // If there is no otherRangeID, it comes back as the number 0.
  renderRangeID(otherRangeID: Long | number) {
    const fixedOtherRangeID = FixLong(otherRangeID);
    const fixedCurrentRangeID = FixLong(this.props.rangeID);
    if (fixedOtherRangeID.eq(0)) {
      return null;
    }

    if (fixedCurrentRangeID.eq(fixedOtherRangeID)) {
      return `r${fixedOtherRangeID.toString()}`;
    }

    return (
      <a href={`#/reports/range/${fixedOtherRangeID.toString()}`}>
        r{fixedOtherRangeID.toString()}
      </a>
    );
  }

  renderLogInfoDescriptor(title: string, desc: string) {
    if (_.isEmpty(desc)) {
      return null;
    }
    return (
      <li>
        {title}: {desc}
      </li>
    );
  }

  renderLogInfo(
    info: protos.znbase.server.serverpb.RangeLogResponse.IPrettyInfo,
  ) {
    return (
      <ul className="log-entries-list">
        {this.renderLogInfoDescriptor("Updated Range Descriptor", info.updated_desc)}
        {this.renderLogInfoDescriptor("New Range Descriptor", info.new_desc)}
        {this.renderLogInfoDescriptor("Added Replica", info.added_replica)}
        {this.renderLogInfoDescriptor("Removed Replica", info.removed_replica)}
        {this.renderLogInfoDescriptor("Reason", info.reason)}
        {this.renderLogInfoDescriptor("Details", info.details)}
      </ul>
    );
  }

  renderContent = () => {
    const { log } = this.props;

    // Sort by descending timestamp.
    const events = _.orderBy(
      log && log.data && log.data.events,
      event => TimestampToMoment(event.event.timestamp).valueOf(),
      "desc",
    );

    return (
      <table className="log-table">
        <tbody>
          <tr className="log-table__row log-table__row--header">
            <th className="log-table__cell log-table__cell--header">时间戳</th>
            <th className="log-table__cell log-table__cell--header">存储</th>
            <th className="log-table__cell log-table__cell--header">事件类型</th>
            <th className="log-table__cell log-table__cell--header">Range</th>
            <th className="log-table__cell log-table__cell--header">其他 Range</th>
            <th className="log-table__cell log-table__cell--header">信息</th>
          </tr>
          {_.map(events, (event, key) => (
            <tr key={key} className="log-table__row">
              <td className="log-table__cell log-table__cell--date">
                {Print.Timestamp(event.event.timestamp)}
              </td>
              <td className="log-table__cell">s{event.event.store_id}</td>
              <td className="log-table__cell">{printLogEventType(event.event.event_type)}</td>
              <td className="log-table__cell">{this.renderRangeID(event.event.range_id)}</td>
              <td className="log-table__cell">{this.renderRangeID(event.event.other_range_id)}</td>
              <td className="log-table__cell">{this.renderLogInfo(event.pretty_info)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  }

  render() {
    const { log } = this.props;

    return (
      <div>
        <h2>Range 日志</h2>
        <Loading
          loading={!log || log.inFlight}
          error={log && log.lastError}
          render={this.renderContent}
        />
      </div>
    );
  }
}
