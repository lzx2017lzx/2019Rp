package day728

object CrediCard {

  //define a variable to store card number
  private[this] var crediCardNumber: Long = 0

  def generateCCNumber():Long = {
    crediCardNumber += 1
    crediCardNumber
  }

  //test program
  def main(args:Array[String]):Unit={
    // produce new card
    println(CrediCard.generateCCNumber())
    println(CrediCard.generateCCNumber())
    println(CrediCard.generateCCNumber())
    println(CrediCard.generateCCNumber())
    println(CrediCard.generateCCNumber())
    println(CrediCard.generateCCNumber())
  }
}
