
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";

import { refreshNodes, refreshLocations } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { selectLocalityTree, LocalityTier, LocalityTree } from "src/redux/localities";
import { selectLocationsRequestStatus, selectLocationTree, LocationTree } from "src/redux/locations";
import { selectNodeRequestStatus } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { findMostSpecificLocation, hasLocation } from "src/util/locations";
import { getNodeLocalityTiers } from "src/util/localities";
import Loading from "src/views/shared/components/loading";

import "./localities.styl";

function formatCoord(coordinate: number) {
  return coordinate.toFixed(4);
}

function renderLocation(locations: LocationTree, tiers: LocalityTier[]) {
  const location = findMostSpecificLocation(locations, tiers);

  if (_.isNil(location)) {
    return "";
  }

  return `${formatCoord(location.latitude)}, ${formatCoord(location.longitude)}`;
}

function renderLocalityTree(locations: LocationTree, tree: LocalityTree) {
  let rows: React.ReactNode[] = [];
  const leftIndentStyle = {
    "padding-left": 20 * tree.tiers.length,
  };

  tree.nodes.forEach((node) => {
    const tiers = getNodeLocalityTiers(node);

    rows.push(
      <tr>
        <td></td>
        <td>n{ node.desc.node_id } @ { node.desc.address.address_field }</td>
        <td className="parent-location">{ renderLocation(locations, tiers) }</td>
      </tr>,
    );
  });

  Object.keys(tree.localities).forEach((key) => {
    Object.keys(tree.localities[key]).forEach((value) => {
      const child = tree.localities[key][value];

      rows.push(
        <tr>
          <td><span style={leftIndentStyle}>{ key }={ value }</span></td>
          <td></td>
          <td className={hasLocation(locations, { key, value }) ? "own-location" : "parent-location"}>
            { renderLocation(locations, child.tiers) }
          </td>
        </tr>,
      );

      rows = rows.concat(renderLocalityTree(locations, child));
    });
  });

  return rows;
}

interface LocalitiesProps {
  localityTree: LocalityTree;
  localityStatus: CachedDataReducerState<any>;
  locationTree: LocationTree;
  locationStatus: CachedDataReducerState<any>;
  refreshLocations: typeof refreshLocations;
  refreshNodes: typeof refreshNodes;
}

class Localities extends React.Component<LocalitiesProps, {}> {
  componentWillMount() {
    this.props.refreshLocations();
    this.props.refreshNodes();
  }

  componentWillReceiveProps(props: LocalitiesProps) {
    props.refreshLocations();
    props.refreshNodes();
  }

  render() {
    return (
      <div>
        <Helmet>
          <title>位置</title>
        </Helmet>
        <section className="section"><h1>所在地</h1></section>
        <Loading
          loading={ !this.props.localityStatus.data || !this.props.locationStatus.data }
          error={ [this.props.localityStatus.lastError, this.props.locationStatus.lastError] }
          render={() => (
            <section className="section">
              <table className="locality-table">
                <thead>
                  <tr>
                    <th>所在地</th>
                    <th>节点</th>
                    <th>位置</th>
                  </tr>
                </thead>
                <tbody>
                  { renderLocalityTree(this.props.locationTree, this.props.localityTree) }
                </tbody>
              </table>
            </section>
          )}
        />
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    localityTree: selectLocalityTree(state),
    localityStatus: selectNodeRequestStatus(state),
    locationTree: selectLocationTree(state),
    locationStatus: selectLocationsRequestStatus(state),
  };
}

const actions = {
  refreshLocations,
  refreshNodes,
};

export default connect(mapStateToProps, actions)(Localities);
