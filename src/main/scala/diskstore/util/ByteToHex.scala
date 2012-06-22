package diskstore.util

import annotation.switch

/**
 * User: gabriel
 * Date: 6/15/12
 */

 object ByteToHex {

 	private def toHexDigit(v:Int)= (v: @switch) match {
 	  case 0 => "0"
  	case 1 => "1"
 	  case 2 => "2"
 	  case 3 => "3"
 	  case 4 => "4"
 	  case 5 => "5"
 	  case 6 => "6"
 	  case 7 => "7"
 	  case 8 => "8"
 	  case 9 => "9"
 	  case 10 => "A"
 	  case 11 => "B"
 	  case 12 => "C"
 	  case 13 => "D"
 	  case 14 => "E"
 	  case 15 => "F"
 	}



 	def apply(b:Byte)={
    val abs=if (b<0) 256+b else b
 		toHexDigit(abs>>>4)+toHexDigit(abs & 0xF)
 	}
 }