import { Component } from '@angular/core';
import {
  NavController,
  ModalController
} from 'ionic-angular';
import { Geolocation } from 'ionic-native';
import UnitModal from "./unit_modal";

@Component({
  templateUrl: 'build/pages/map/map.html'
})
export class MapPage {

  map: L.Map;
  unit: string;
  unitLayer: any;
  unitProperties: any;
  watchPositionSubscription: any;
  currentLocationCircle: L.Circle;
  currentLocationMarker: L.Marker;
  lastPosition: any;

  constructor(public navCtrl: NavController, public modalCtrl: ModalController) {
  }

  // Other ionic lifecycle methods at http://stackoverflow.com/questions/34944856/event-to-loaded-page-content-ionic-2#34945653
  ionViewLoaded( ) {
    this.map = L.map( "map", { zoomControl: false } );
    const stamenTonerLite = L.tileLayer('http://{s}.tile.stamen.com/toner-lite/{z}/{x}/{y}.png', {
      attribution: 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>',
      subdomains: 'abcd',
      minZoom: 0,
      maxZoom: 20
    });
    stamenTonerLite.addTo( this.map );
    this.map.setView( [37.7185, -122.34375], 11 );
    this.unitLayer = Tangram.leafletLayer( {
      interactive: true,
      scene: './underfoot-mvt-scene.yaml',
      attribution: '<a href="https://mapzen.com/tangram" target="_blank">Tangram</a> | &copy; OSM contributors | <a href="https://mapzen.com/" target="_blank">Mapzen</a>'
    } );
    this.unitLayer.addTo( this.map );
    this.map.on( "moveend", e => {
      this.showUnit( e );
    } );
    this.map.on( "zoomend", e => {
      this.showUnit( e );
    } );
    this.watchCurrentLocation( );
  }

  ionicViewUnloaded( ) {
    this.stopWatchCurrentLocation( );
  }

  ionViewDidEnter( ) {
    // if you don't do this the tiles are kind of offset
    this.map.invalidateSize( false );
  }

  zoomIn( ) {
    this.map.zoomIn( );
  }

  zoomOut( ) {
    this.map.zoomOut( );
  }

  showCurrentLocation( ) {
    if( this.lastPosition ) {
      this.showPosition( this.lastPosition );
      this.zoomToPosition( this.lastPosition );
      return;
    }
    Geolocation.getCurrentPosition( { enableHighAccuracy: true } )
      .then(
        position => {
          this.showPosition( position );
          this.zoomToPosition( position );
        },
        this.onMapError
      ); 
  }

  showPosition( position ) {
    const latLng = L.latLng( position.coords.latitude, position.coords.longitude );
    if ( !this.currentLocationMarker ) {
      this.currentLocationMarker = L.marker( latLng, {
        icon: L.divIcon( { className: "current-location" } ),
        interactive: false
      } ).addTo( this.map );
    }
    this.currentLocationMarker.setLatLng( latLng );
  }

  zoomToPosition( position ) {
    const latLng = L.latLng( position.coords.latitude, position.coords.longitude );
    this.map.flyTo( latLng, 16 );
  }

  watchCurrentLocation( ) {
    this.showCurrentLocation( );
    this.watchPositionSubscription = Geolocation.watchPosition( { enableHighAccuracy: true } )
      .subscribe( position => {
        this.lastPosition = position;
        this.showPosition( position );
      }, this.onMapError );
  }

  stopWatchCurrentLocation() {
    if ( !this.watchPositionSubscription ) { return };
    this.watchPositionSubscription.unsubscribe( );
  }

  onMapError( e: any ) {
    alert( "Couldn't find your location: " + e );
  }

  showUnit( e: any ) {
    const center = this.map.getCenter( );
    var mapWidth = this.map.getContainer( ).offsetWidth;
    var mapHeight = this.map.getContainer( ).offsetHeight;
    var px = { x: mapWidth / 2, y: mapHeight / 2 };
    this.unitLayer.scene.getFeatureAt( px ).then( selection => {
      console.log("selection: ", selection);
      if (
        !selection || 
        !selection.feature ||
        !selection.feature.properties ||
        !selection.feature.properties.code
       ) {
        this.unit = "No unit found";
        return;
      }
      this.unitProperties = selection.feature.properties;
      this.unit = `${selection.feature.properties.title} (${selection.feature.properties.code})`;
    } );
  }

  presentUnitModal( ) {
    let unitModal = this.modalCtrl.create( UnitModal, { unitProperties: this.unitProperties } );
    unitModal.present();
  }

}
