import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.Random
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.BigInt
import java.security.MessageDigest

object project3 {
	def main(args: Array[String]){

		case class CreateTopology()
		case class Join(nodeId: String) 
		case class Search(hashvalue: BigInt)

		println("project3 - Chord DHT")
		class Master(numOfNodes: Int, numOfRequests: Int) extends Actor{

			//var gossipList: ArrayBuffer[String] = new ArrayBuffer[String]

			def receive = {
				case CreateTopology() =>
					var temp: String = "start"
					for(i <- 0 until numOfNodes){
							var myID: String = "ipaddress".concat(i.toString)
							val act = context.actorOf(Props(new Node(myID, numOfRequests)),name=myID)
							act ! Join(temp)
							temp = myID
						}
			}
		}

		class Node(myID: String, numOfRequests: Int) 
			extends Actor{

			var predecessor: String =_
			var fingertable: Array[String] = new Array[String](160)

			// find id's successor
			def findSuccessor(id: BigInt) = {
				var node: String = findPredecessor(id)
				context.actorSelection("../"+node) ! "TellSuccessor"
			}

			def findPredecessor(id: BigInt) = {

			}

			def receive = {
				case Join(nodeId: String) =>
					if(nodeId == "start") {
						predecessor = myID
						for(i <- 0 until 160){
							fingertable(i) = myID
						}
					} else {
						// Update predeccesor and finger table
						context.actorSelection("../" + nodeId) ! Search(sha1(myID)+1)
						// Tell others to update their predecessors and finger tables
					}
				case Search(id: BigInt) =>
					var node: String = findSuccessor(id)
					sender ! FoundSuccessor(node)

				case FoundSearch(node: String) =>

				case "TellSuccessor" =>
					sender ! FoundSuccessor(fingertable[0])

			}
		}

		def sha256(s: String): String = {
			val md = MessageDigest.getInstance("SHA-1")
			val digest: Array[Byte] = md.digest(s.getBytes)
			var sb: StringBuffer = new StringBuffer
			digest.foreach { digest =>
				var hex = Integer.toHexString(digest & 0xff)
				if (hex.length == 1) sb.append('0')
				sb.append(hex)
			}
			sb.toString()
		}
		println(sha256("ipaddress5"))

		def sha1(s: String): BigInt = {
			val md = MessageDigest.getInstance("SHA-1")
			val digest: Array[Byte] = md.digest(s.getBytes)
			var sb: StringBuffer = new StringBuffer
			digest.foreach { digest =>
				var intvalue = digest.toInt
				println(intvalue)
				sb.append(intvalue)
			}
			val str: String = sb.toString()
			println(str)
			BigInt(str)

		}

		println(BigInt("1234567890"))
		println(sha1("ipaddress5"))


		if(args.size<2){
			println("Enter valid number of inputs!!")
		} else {
			val system = ActorSystem("MasterSystem")
			val master = system.actorOf(Props(new Master(args(0).toInt, args(1).toInt)), name = "master")
			master ! CreateTopology()
		}

	}	
}