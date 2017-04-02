//import java.io._
class Point(val xc:Int, val yc:Int){
	var x:Int=xc;
	var y:Int=yc;
	def move(dx:Int, dy:Int){
		x=x+dx;
		y=y+dy;
		println("Point x location "+x);
		println("Point y location "+y);
	}
}
object HelloWorld{
	def main(args: Array[String]){
		println("HelloWorld!");
		val myVal:Int = 10;
		var myVar:Float=3.1415926f;
		println(f"my var is $myVar%.3f");
		val pt=new Point(10,20);
		pt.move(1,1);
	}
}