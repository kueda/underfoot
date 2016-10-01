import { Component } from '@angular/core';
import { ViewController, NavParams } from 'ionic-angular';
import * as _ from "lodash";

@Component({
  templateUrl: 'build/pages/map/unit_modal.html'
})
export default class UnitModal {

  unitProperties: any;

  constructor( public viewCtrl: ViewController, navParams: NavParams ) {
    this.unitProperties = navParams.data.unitProperties;
  }

  dismiss( ) {
    this.viewCtrl.dismiss( );
  }

  yearsAgo( lower: number, upper?:number ) {
    let lowerText = "";
    let upperText = "";
    let yearsText = "";
    if ( lower < 1e6 ) {
      lowerText = `${this.numberWithCommas( lower )}`;
    } else if ( lower < 1e9 ) {
      lowerText = `${this.numberWithCommas( _.round( lower / 1e6, 2 ) )} million`;
    } else {
      lowerText = `${this.numberWithCommas( _.round( lower / 1e9, 2 ) )} billion`;
    }
    if ( upper ) {
      if ( upper < 1e6 ) {
        upperText = `to ${this.numberWithCommas( upper )}`;
      } else if ( upper < 1e9 ) {
        upperText = `to ${this.numberWithCommas( _.round( upper / 1e6, 2 ) )} million`;
      } else {
        upperText = `to ${this.numberWithCommas( _.round( upper / 1e9, 2 ) )} billion`;
      }
    }
    return [lowerText, upperText, "years ago"].join( " " )
  }

  // http://stackoverflow.com/a/2901298/720268
  numberWithCommas( x ) {
    const parts = x.toString( ).split( "." );
    parts[0] = parts[0].replace( /\B(?=(\d{3})+(?!\d))/g, "," );
    return parts.join( "." );
  }
}