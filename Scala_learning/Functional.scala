object Functional{
	def square(x:Int) :Int={
		return x*x;
	}
	def func(a:Int, f: Int=>Int) :Int={
		return f(a);
	}
	def transform(s:String, f:String=>String) : String= {
		return f(s);
	} 
	def main(args: Array[String]){
		println(mul(4));
		println(func(3,square));
		var s:String="aslhfaUWIY";
		println(transform(s,s=>s.toUpperCase()));
	}
	var factor=3;
	val mul = (i:Int) => i*factor;
}