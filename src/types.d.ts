declare module "*.yaml" {
  const content: string;
  export default content;
}

declare module "*?raw" {
  const content: string;
  export default content;
}
