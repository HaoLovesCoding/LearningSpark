import Array._
object Demo {
	def test(a:Int) : Int ={
		var res:Int=0;
		a match {
			case 1 => res=1;
			case 2 => res=2;
			case _ => res=3;   
		}
		println("res is"+res);
		return res;
	}
	def main(args:Array[String]){
		var myList = Array(1.9,2.9,3.4,3.5);
		for( x <- myList) {
			println(x);
		}
		var total : Double=0;
		for( i <- 0 to myList.length-1) {
			total+=myList(i);
		}
		println("total is "+total);
		var myMat = ofDim[Int](3,3);
		for( i <- 0 to 2) {
			for( j <- 0 to 2) {
				myMat(i)(j)=i+j;
			}
		}
		println(myMat(1)(1));
		var mySec = myMat;
		myMat(1)(1)=1000;
		println(mySec(1)(1));
		test(100);
		test(2);
	}
}