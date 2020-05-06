class BankAccount(private var _balance:Double = 0.0) {
  def deposit(value:Int) {
    _balance += value
  }
  def withdraw(value:Int) {
    if (_balance - value > 0) {
      _balance -= value
    }
  }
  def balance = _balance
}

object Main {
  def matchTest(x: Any): Any = x match {
    case 1 => "one"
    case "two" => 2
    case y: Int => "Int"
    case _ => "String"
  }


  def main(args: Array[String]) {
    val myBA = new BankAccount
    myBA.deposit(100)
    myBA.withdraw(20)
    println(myBA.balance)
  }
}

class Car(val manufacturer: String, val modelName: String) {
  var licencePlate: String = ""
  private var _modelYear: Int = -1
  def this(manufacturer: String, modelName: String, modelYear: Int) {
    this(manufacturer, modelName)
    this._modelYear = modelYear
  }
  def this(manufacturer: String, modelName: String, licencePlate: String) {
    this(manufacturer, modelName)
    this.licencePlate = licencePlate
  }
  def this(manufacturer: String, modelName: String, modelYear: Int, licencePlate: String) {
    this(manufacturer, modelName)
    this._modelYear = modelYear
    this.licencePlate = licencePlate
  }
  def modelYear = _modelYear
}


