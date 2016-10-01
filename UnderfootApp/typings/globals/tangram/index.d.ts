// I added this on 2016-09-24, it's not from typings
declare namespace Tangram {
  export function leafletLayer(options: Object): L.Layer;
}

declare module 'Tangram' {
  export = Tangram;
}