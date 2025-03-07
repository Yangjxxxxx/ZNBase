  
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
import Helmet from "react-helmet";
import { withRouter, WithRouterProps } from "react-router";
import { connect } from "react-redux";

import { enqueueRange } from "src/util/api";
import { znbase } from "src/js/protos";
import Print from "src/views/reports/containers/range/print";
import "./index.styl";

import EnqueueRangeRequest = znbase.server.serverpb.EnqueueRangeRequest;
import EnqueueRangeResponse = znbase.server.serverpb.EnqueueRangeResponse;

const QUEUES = [
  "replicate",
  "gc",
  "merge",
  "split",
  "replicaGC",
  "raftlog",
  "raftsnapshot",
  "consistencyChecker",
  "timeSeriesMaintenance",
];

interface EnqueueRangeProps {
  handleEnqueueRange: (queue: string, rangeID: number, nodeID: number, skipShouldQueue: boolean) => Promise<EnqueueRangeResponse>;
}

interface EnqueueRangeState {
  queue: string;
  rangeID: string;
  nodeID: string;
  skipShouldQueue: boolean;
  response: EnqueueRangeResponse;
  error: Error;
}

class EnqueueRange extends React.Component<EnqueueRangeProps & WithRouterProps, EnqueueRangeState> {
  state: EnqueueRangeState = {
    queue: QUEUES[0],
    rangeID: "",
    nodeID: "",
    skipShouldQueue: false,
    response: null,
    error: null,
  };

  handleUpdateQueue = (evt: React.FormEvent<{ value: string }>) => {
    this.setState({
      queue: evt.currentTarget.value,
    });
  }

  handleUpdateRangeID = (evt: React.FormEvent<{ value: string }>) => {
    this.setState({
      rangeID: evt.currentTarget.value,
    });
  }

  handleUpdateNodeID = (evt: React.FormEvent<{ value: string }>) => {
    this.setState({
      nodeID: evt.currentTarget.value,
    });
  }

  handleSubmit = (evt: React.FormEvent<any>) => {
    evt.preventDefault();

    this.props.handleEnqueueRange(
      this.state.queue,
      // These parseInts should succeed because <input type="number" />
      // enforces numeric input. Otherwise they're NaN.
      _.parseInt(this.state.rangeID),
      _.parseInt(this.state.nodeID),
      this.state.skipShouldQueue,
    ).then(
      (response) => {
        this.setState({ response: response, error: null });
      },
      (err) => {
        this.setState({ response: null, error: err });
      },
    );
  }

  renderNodeResponse(details: EnqueueRangeResponse.IDetails) {
    return (
      <React.Fragment>
        <p>
          {details.error
            ? <React.Fragment><b>错误:</b> {details.error}</React.Fragment>
            : "Call succeeded"}
        </p>
        <table className="enqueue-range-table">
          <thead>
            <tr className="enqueue-range-table__row enqueue-range-table__row--header">
              <th className="enqueue-range-table__cell enqueue-range-table__cell--header">时间戳</th>
              <th className="enqueue-range-table__cell enqueue-range-table__cell--header">信息</th>
            </tr>
          </thead>
          <tbody>
            {details.events.map((event) => (
              <tr className="enqueue-range-table__row--body">
                <td className="enqueue-range-table__cell enqueue-range-table__cell--date">
                  {Print.Timestamp(event.time)}
                </td>
                <td className="enqueue-range-table__cell">
                  <pre>{event.message}</pre>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </React.Fragment>
    );
  }

  renderResponse() {
    const { response } = this.state;

    if (!response) {
      return null;
    }

    return (
      <React.Fragment>
        <h2>Enqueue Range 输出</h2>
        {response.details.map((details) => (
          <div>
            <h3>节点 n{details.node_id}</h3>

            {this.renderNodeResponse(details)}
          </div>
        ))}
      </React.Fragment>
    );
  }

  renderError() {
    const { error } = this.state;

    if (!error) {
      return null;
    }

    return (
      <React.Fragment>运行Enqueue Range错误: { error.message }</React.Fragment>
    );
  }

  render() {
    return (
      <React.Fragment>
        <Helmet>
          <title>Enqueue Range</title>
        </Helmet>
        <div className="content">
          <section className="section">
            <div className="form-container">
              <h1 className="heading">手动在替换队列中排序Range</h1>
              <br />
              <form onSubmit={this.handleSubmit} className="form-internal" method="post">
                <label>
                  队列:{" "}
                  <select onChange={this.handleUpdateQueue}>
                    {QUEUES.map((queue) => (
                      <option key={queue} value={queue}>{queue}</option>
                    ))}
                  </select>
                </label>
                <br />
                <label>
                  RangeID:{" "}
                  <input
                    type="number"
                    name="rangeID"
                    className="input-text"
                    onChange={this.handleUpdateRangeID}
                    value={this.state.rangeID}
                    placeholder="RangeID"
                  />
                </label>
                <br />
                <label>
                  节点ID:{" "}
                  <input
                    type="number"
                    name="nodeID"
                    className="input-text"
                    onChange={this.handleUpdateNodeID}
                    value={this.state.nodeID}
                    placeholder="NodeID (optional)"
                  />
                  &nbsp;如果未指定，我们将尝试加入所有节点
                </label>
                <br />
                <label>
                  跳过队列:{" "}
                  <input
                    type="checkbox"
                    checked={this.state.skipShouldQueue}
                    name="skipShouldQueue"
                    onChange={() => this.setState({ skipShouldQueue: !this.state.skipShouldQueue })}
                  />
                </label>
                <br />
                <input
                  type="submit"
                  className="submit-button"
                  value="Submit"
                />
              </form>
            </div>
          </section>
        </div>
        <section className="section">
          {this.renderResponse()}
          {this.renderError()}
        </section>
      </React.Fragment>
    );
  }
}

// tslint:disable-next-line:variable-name
const EnqueueRangeConnected = connect(
  () => {
    return {};
  },
  () => ({
    handleEnqueueRange: (queue: string, rangeID: number, nodeID: number, skipShouldQueue: boolean) => {
      const req = new EnqueueRangeRequest({
        queue: queue,
        range_id: rangeID,
        node_id: nodeID,
        skip_should_queue: skipShouldQueue,
      });
      return enqueueRange(req);
    },
  }),
)(withRouter(EnqueueRange));

export default EnqueueRangeConnected;
