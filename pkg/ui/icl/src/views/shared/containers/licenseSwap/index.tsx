
import _ from "lodash";
import React from "react";
import { connect } from "react-redux";

//import { selectEnterpriseEnabled } from "src/redux/license";
import { AdminUIState } from "src/redux/state";

// Some of the type magic is adapted from @types/react-redux.
// Some of the code is adapted from react-redux.

type ComponentClass<P> = React.ComponentClass<P>;
type StatelessComponent<P> = React.StatelessComponent<P>;
type Component<P> = ComponentClass<P> | StatelessComponent<P>;

function getComponentName<P>(wrappedComponent: Component<P>) {
  return wrappedComponent.displayName
    || wrappedComponent.name
    || "Component";
}

// function combineNames(a: string, b: string) {
//   if (a === b) {
//     return a;
//   }

//   return a + "," + b;
// }

interface OwnProps {
  enterpriseEnabled: boolean;
}

function mapStateToProps<T>(state: AdminUIState, _ownProps: T) {
  return {
    enterpriseEnabled: true,
  };
}

/**
 * LicenseSwap is a higher-order component that swaps out two components based
 * on the current license status.
 */
export default function swapByLicense(
  // tslint:disable:variable-name
  //OSSComponent: Component<TProps>,
  ICLComponent: Component<any>,
  // tslint:enable:variable-name
): ComponentClass<any> {
  //const ossName = getComponentName(OSSComponent);
  const iclName = getComponentName(ICLComponent);

  class LicenseSwap extends React.Component<any & OwnProps, {}> {
    // public static displayName = `LicenseSwap(${combineNames(ossName, cclName)})`;
    public static displayName = `${iclName}`;
    render() {
      const props = _.omit(this.props, ["enterpriseEnabled"]);

      return <ICLComponent {...props} />;
    }
  }

  return connect(mapStateToProps)(LicenseSwap);
}
