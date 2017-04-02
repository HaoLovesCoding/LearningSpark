class Try{
	def add(a:Int,b:Int) : Int={
		var sum:Int=0;
		sum=a+b;
		println(sum);
		return sum;
	}
}
object something {
	def main(args: Array[String]){
		var sth=new Try();
		sth.add(1,3);
		var i=0;
		for(a<-1 to 10){
			println(a);
		}
	}
}