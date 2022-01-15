
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import React from "react";

import DebugAnnotation from "src/views/shared/components/debugAnnotation";

/**
 * LicenseType is an indicator showing the current build license.
 */
export default class LicenseType extends React.Component<{}, {}> {
  render() {
    return <DebugAnnotation label="License type" value="OSS" />;
  }
}
