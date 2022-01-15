 

import React from "react";
import moment from "moment";
import { Link } from "react-router";

import { INodeStatus } from "src/util/proto";
import { nodeCapacityStats, livenessNomenclature } from "src/redux/nodes";
import { trustIcon } from "src/util/trust";
import liveIcon from "!!raw-loader!assets/livenessIcons/live.svg";
import suspectIcon from "!!raw-loader!assets/livenessIcons/suspect.svg";
import deadIcon from "!!raw-loader!assets/livenessIcons/dead.svg";
import nodeIcon from "!!raw-loader!assets/nodeIcon.svg";
import { Labels } from "src/views/clusterviz/components/nodeOrLocality/labels";
import { CapacityArc } from "src/views/clusterviz/components/nodeOrLocality/capacityArc";
import { Sparklines } from "src/views/clusterviz/components/nodeOrLocality/sparklines";
import { LongToMoment } from "src/util/convert";
import { znbase } from "src/js/protos";

import NodeLivenessStatus = znbase.storage.NodeLivenessStatus;
type ILiveness = znbase.storage.ILiveness;

interface NodeViewProps {
  node: INodeStatus;
  livenessStatus: NodeLivenessStatus;
  liveness: ILiveness;
}

const SCALE_FACTOR = 0.8;
const TRANSLATE_X = -90 * SCALE_FACTOR;
const TRANSLATE_Y = -100 * SCALE_FACTOR;

export class NodeView extends React.Component<NodeViewProps> {
  getLivenessIcon(livenessStatus: NodeLivenessStatus) {
    switch (livenessStatus) {
      case NodeLivenessStatus.LIVE:
        return liveIcon;
      case NodeLivenessStatus.DEAD:
        return deadIcon;
      default:
        return suspectIcon;
    }
  }

  getUptimeText() {
    const { node, livenessStatus, liveness } = this.props;

    switch (livenessStatus) {
      case NodeLivenessStatus.DEAD: {
        if (!liveness) {
          return "失效";
        }

        const deadTime = liveness.expiration.wall_time;
        const deadMoment = LongToMoment(deadTime);
        return `失效了 ${moment.duration(deadMoment.diff(moment())).humanize()}`;
      }
      case NodeLivenessStatus.LIVE: {
        const startTime = LongToMoment(node.started_at);
        return `活跃了 ${moment.duration(startTime.diff(moment())).humanize()}`;
      }
      default:
        return livenessNomenclature(livenessStatus);
    }
  }

  render() {
    const { node, livenessStatus } = this.props;
    const { used, usable } = nodeCapacityStats(node);

    return (
      <Link
        to={`/node/${node.desc.node_id}`}
        style={{ cursor: "pointer" }}
      >
        <g transform={`translate(${TRANSLATE_X},${TRANSLATE_Y})scale(${SCALE_FACTOR})`}>
          <rect width={180} height={210} opacity={0} />
          <Labels
            label={`节点 ${node.desc.node_id}`}
            subLabel={this.getUptimeText()}
            tooltip={node.desc.address.address_field}
          />
          <g dangerouslySetInnerHTML={trustIcon(nodeIcon)} transform="translate(14 14)" />
          <g
            dangerouslySetInnerHTML={trustIcon(this.getLivenessIcon(livenessStatus))}
            transform="translate(9, 9)"
          />
          <CapacityArc
            usableCapacity={usable}
            usedCapacity={used}
          />
          <Sparklines nodes={[`${node.desc.node_id}`]} />
        </g>
      </Link>
    );
  }
}
