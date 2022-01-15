 

// TODO(mrtracy): Convert the JSON files into JS files to have them obtain types
// directly.
declare module "*.json" {
    const value: any;
    export default value;
}
