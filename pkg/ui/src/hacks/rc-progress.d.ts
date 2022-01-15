
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// TODO(benesch): upstream this.

declare module "rc-progress" {
  export interface LineProps {
    strokeColor?: string;
    strokeWidth?: number;
    trailWidth?: number;
    className?: string;
    percent?: number;
  }
  export class Line extends React.Component<LineProps, {}> {
  }
}
