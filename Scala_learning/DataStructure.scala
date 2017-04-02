object DataStructure{
	def main(arg: Array[String]) = {
		val myTuple=("A","B","C")
		println(myTuple._1)
		println(myTuple._2)

		val myPair="Picard"->"Enterprise"
		println(myPair._2)

		val myList: List[String]=List("AQ","BQ","CQ")
		println(myList(2))
		for( item <- myList) {
			println(item)
		}

		println(myList.head)
		println("tail")
		println(myList.tail)

		val backward = myList.map( (item:String) => (item.reverse) )
		//reverse the string, not reverse the list

		for( back <- backward) {
			println(back)
		}

		val myNumberList = List(1,2,3,4,5)
		val sum=myNumberList.reduce((x:Int,y:Int)=>x+y)
		println("sum"+sum)

		val iHateFive = myNumberList.filter((x:Int) => x!=5 )
		for( hate <- iHateFive) {
			println(hate)
		}

		val iHateThree= myNumberList.filter(_!=3)

		val myNum1 = List(1,2,3,4)
		val myNum2 = List(4,5,6,7)
		val myNum3 = myNum1 ++ myNum2

		println(myNum3.min)
		println(myNum3.max)
		println(myNum3.sum)

		val myMap= Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D")
		println(myMap("Kirk"))

		val acherShip = util.Try(myMap("archer")) getOrElse "Unknown"
		println(acherShip)

		var xList : List[Int]= List.fill(10)(1)

		var yList : List[Int]= List.range(1,21,1)
		for(  y<- yList) {
			println(y)
		}
		var yList_new = yList.filter((x:Int) => x%3!=0)
		for( yy <- yList_new) {
			println(yy)
		}
	}
}