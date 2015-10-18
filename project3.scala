import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.Random
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.math.BigInt
import java.security.MessageDigest

object project3 {
	def main(args: Array[String]){

		case class Initialize()
		case class AddNextNode(sender: String)
		case class Join(nodeId: String) 
		case class SearchPreAndSucc(key: BigInt, sender: String)
		case class FoundPreAndSucc(pre: String, succ: String)
		case class UpdatePredecessor(update: String)

		println("project3 - Chord DHT")
		class Master(numOfNodes: Int, numOfRequests: Int) extends Actor{

			//var gossipList: ArrayBuffer[String] = new ArrayBuffer[String]
			var count: Int=_
			def receive = {
				case Initialize() =>
					var myID: String = "ipaddress0"
					println("Creating first node with ip = " + myID)
					val act = context.actorOf(Props(new Node(myID, numOfRequests, self)),name=myID)
					act ! Join("start")  // TODO

				case AddNextNode(sender: String) =>
					count += 1
					if(count < numOfNodes) {
						var myID: String = "ipaddress".concat(count.toString)
						println("Creating node with ip = " + myID)
						val act = context.actorOf(Props(new Node(myID, numOfRequests, self)),name=myID)
						act ! Join(sender)
					} else {
						println("Done with adding nodes to the network!!")
						context.system.shutdown()
					}
					 

			}
		}

		class Node(myID: String, numOfRequests: Int, masterRef:ActorRef) 
			extends Actor{

			var predecessor: String =_
			var fingertable: Array[String] = new Array[String](160)

			def receive = {
				case Join(friend: String) =>
					if(friend == "start") {
						predecessor = myID
						for(i <- 0 until 160){
							fingertable(i) = myID
						}
						sender ! AddNextNode(myID)
					} else {
						// Find predeccesor and Successor; for that ask some existing
						// node present in the network
						println("SearchPreAndSucc - " + myID + " asking " + friend)
						context.actorSelection("../" + friend) ! SearchPreAndSucc(sha1(myID)+1,myID)
					}
				case SearchPreAndSucc(key: BigInt, sender: String) =>
					// 1st find predecessor of key before finding the successor which is trivial
					// once found the predecessor
					println("SearchPreAndSucc - " + myID + " got from " + sender)
					if(key > (sha1(myID)) || key < sha1(fingertable(0))){
						context.actorSelection("../"+sender) ! FoundPreAndSucc(myID,fingertable(0))
						fingertable(0) = sender
					} else {
						println("SearchPreAndSucc - " + myID + " asking " + fingertable(0))
						context.actorSelection("../"+fingertable(0)) ! SearchPreAndSucc(key,sender)
					}

				case FoundPreAndSucc(pre: String, succ: String) =>
					predecessor = pre
					fingertable(0) = succ
					context.actorSelection("../" + fingertable(0)) ! UpdatePredecessor(myID) 

				case UpdatePredecessor(update: String) =>
					predecessor = update
					masterRef ! AddNextNode(myID)
			}
		}

		def sha1(s: String): BigInt = {
			val md = MessageDigest.getInstance("SHA-1")
			val bytes: Array[Byte] = md.digest(s.getBytes)
			var sb: StringBuffer = new StringBuffer
			var bi: BigInt = new BigInt(new java.math.BigInteger(bytes))
			return bi
		}

		// def sha1(s: String): BigInt = {
		// 	val md = MessageDigest.getInstance("SHA-1")
		// 	val digest: Array[Byte] = md.digest(s.getBytes)
		// 	var sb: StringBuffer = new StringBuffer
		// 	digest.foreach { digest =>
		// 		var hex = Integer.toHexString(digest & 0xff)
		// 		if (hex.length == 1) sb.append('0')
		// 		sb.append(hex)
		// 	}
		// 	val str: String = sb.toString()
		// 	println(str)
		// 	BigInt(str)

		// }

		if(args.size<2){
			println("Enter valid number of inputs!!")
		} else {
			val system = ActorSystem("MasterSystem")
			val master = system.actorOf(Props(new Master(args(0).toInt, args(1).toInt)), name = "master")
			master ! Initialize()
		}

	}	
}