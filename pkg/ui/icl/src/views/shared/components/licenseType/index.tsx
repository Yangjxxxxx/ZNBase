import React from "react";

import DebugAnnotation from "src/views/shared/components/debugAnnotation";
import swapByLicense from "src/views/shared/containers/licenseSwap";
//import OSSLicenseType from "oss/src/views/shared/components/licenseType";

class ICLLicenseType extends React.Component<{}, {}> {
  render() {
    return <DebugAnnotation label="证书类型" value="ICL" />;
  }
}

/**
 * LicenseType is an indicator showing the current build license.
 */
// tslint:disable-next-line:variable-name
const LicenseType = swapByLicense(ICLLicenseType);

export default LicenseType;
