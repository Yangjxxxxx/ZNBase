import React from "react";
import { Route, IndexRedirect } from "react-router";
import ClusterViz from "src/views/clusterviz/containers/map";
import NodeList from "src/views/clusterviz/containers/map/nodeList";
import ClusterOverview from "src/views/cluster/containers/clusterOverview";

export const CLUSTERVIZ_ROOT = "/overview/map";

export default function createNodeMapRoutes(): JSX.Element {
  return (
    <Route path="overview" component={ ClusterOverview } >
      <IndexRedirect to="list" />
      <Route path="list" component={ NodeList } />
      <Route path="map(/**)" component={ ClusterViz } />
    </Route>
  );
}