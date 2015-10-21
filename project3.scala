import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.Random
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.math.BigInt
import scala.math.pow
import java.security.MessageDigest

object project3 {
	def main(args: Array[String]){

		case class Initialize()
		case class AddNextNode(sender: String)
		
		println("project3 - Chord DHT")
		class Master(numOfNodes: Int, numOfRequests: Int) extends Actor{

			//var gossipList: ArrayBuffer[String] = new ArrayBuffer[String]
			var count: Int=_
			var totalRequests: Int=_
			def receive = {
				case Initialize() =>
					var friend: String = "start"
					var myID: String = "ipaddress0"
					println("Creating first node with ip = " + sha1(myID))
					println("Creating first node with ip = " + sha256(myID))
					val act = context.actorOf(Props(new Node(myID, numOfRequests, friend, self)),name=myID)
					act ! Join()  

				case AddNextNode(sender: String) =>
					count += 1
					if(count < numOfNodes) {
						var myID: String = "ipaddress".concat(count.toString)
						println("Creating node with ip = " + sha1(myID))
						println("Creating node with ip = " + sha256(myID))
						val act = context.actorOf(Props(new Node(myID, numOfRequests, sender, self)),name=myID)
						act ! Join()
					} else {
						println("Done with adding nodes to the network!!")
						//context.system.shutdown()
						for(i <- 0 until numOfNodes){
							var id: String = "ipaddress".concat(i.toString)
							context.actorSelection(id) ! "SendRequests"
						}
					}
					 

			}
		}

		case class Join() 
		case class SearchPreAndSucc(key: BigInt, sender: String)
		case class FoundPreAndSucc(pre: String, succ: String)
		case class UpdatePredecessor(update: String)
		case class InitFingertable(i: Int)
		case class SearchSuccessor(key: BigInt, i: Int, sender: String) 
		case class FoundSuccessor(owner: String, i:Int)
		case class UpdateOthers(i: Int)
		case class SearchPredecessor(key: BigInt, i: Int, sender: String)
		case class FoundPredecessor(pre: String, i: Int)
		case class UpdateFingerTable(update: String, i: Int)
		case class SearchFile(key: BigInt, sender: String, hopcount: Int)
		case class FoundFile(owner: String, key: BigInt, hopcount: Int)


		class Node(myID: String, numOfRequests: Int, friend: String, masterRef:ActorRef) 
			extends Actor{

			var predecessor: String =_
			var fingertable = Array.ofDim[String](160,2)


			def receive = {
				case Join() =>
					if(friend == "start") {
						predecessor = myID
						for(i <- 0 until 160){
							fingertable(i)(0) = convert2Hashvalue(myID,i)
							fingertable(i)(1) = myID
						}
						sender ! AddNextNode(myID)
					} else {
						// Find predeccesor and Successor; for that ask some existing
						// node present in the network
						// println("SearchPreAndSucc - " + myID + " asking " + friend)
						context.actorSelection("../" + friend) ! SearchPreAndSucc(sha1(myID)+1,myID)
					}
				case SearchPreAndSucc(key: BigInt, sender: String) =>
					// 1st find predecessor of key before finding the successor which is trivial
					// once found the predecessor
					// println("SearchPreAndSucc - " + myID + " got from " + sender)
					if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
						context.actorSelection("../"+sender) ! FoundPreAndSucc(myID,fingertable(0)(1))
						fingertable(0)(1) = sender
					} else {
						// println("SearchPreAndSucc - " + myID + " asking " + fingertable(0)(1))
						context.actorSelection("../"+fingertable(0)(1)) ! SearchPreAndSucc(key,sender)
					}

				case FoundPreAndSucc(pre: String, succ: String) =>
					predecessor = pre
					fingertable(0)(1) = succ
					context.actorSelection("../" + fingertable(0)(1)) ! UpdatePredecessor(myID) 

				case UpdatePredecessor(update: String) =>
					predecessor = update
					sender ! InitFingertable(1)
					// masterRef ! AddNextNode(myID)
				
				// Message types for initializing fingertable

				case InitFingertable(i: Int) =>
					if(i<160){
						var key: BigInt = BigInt(convert2Hashvalue(myID,i))
						if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
							fingertable(i)(0) = convert2Hashvalue(myID,i)
							fingertable(i)(1) = fingertable(0)(1)
							self ! InitFingertable(i+1)
						} else {
							context.actorSelection("../" + friend) ! SearchSuccessor(BigInt(convert2Hashvalue(myID,i)),i,myID)
						}
					} else {
						self ! UpdateOthers(0)
						// masterRef ! AddNextNode(myID)
					}

				case SearchSuccessor(key: BigInt, i: Int, sender: String) =>
					if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
						context.actorSelection("../"+sender) ! FoundSuccessor(fingertable(0)(1),i)
					} else {
						context.actorSelection("../"+fingertable(0)(1)) ! SearchSuccessor(key,i,sender)
					}

				case FoundSuccessor(owner: String, i:Int) =>
					fingertable(i)(0) = convert2Hashvalue(myID,i)
					fingertable(i)(1) = owner
					if(160>i+1){
						context.actorSelection("../" + friend) ! SearchSuccessor(BigInt(convert2Hashvalue(myID,i+1)),i+1,myID)
					} else {
						self ! UpdateOthers(0)
						// masterRef ! AddNextNode(myID)
					}

				// Message types for updating other fingertables

				case UpdateOthers(i: Int) =>
					if(160>i){
						var key: BigInt = (sha1(myID) - BigInt(((pow(2,i)).toInt).toString))
						context.actorSelection("../"+fingertable(0)(1)) ! SearchPredecessor(key,i,myID)
					} else {
						masterRef ! AddNextNode(myID)
					}
					
				case SearchPredecessor(key: BigInt, i: Int, sender: String) =>
					if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
						context.actorSelection("../"+sender) ! FoundPredecessor(myID,i)
					} else {
						context.actorSelection("../"+fingertable(0)(1)) ! SearchPredecessor(key,i,sender)
					}

				case FoundPredecessor(pre: String, i: Int) =>
					context.actorSelection("../"+pre) ! UpdateFingerTable(myID,i)

				case UpdateFingerTable(update: String, i: Int) =>
					if(sha1(update) > (sha1(myID)) || sha1(update) < sha1(fingertable(i)(1))){
						fingertable(i)(1) = update
						context.actorSelection("../"+predecessor) ! UpdateFingerTable(update,i)
					} else {
						context.actorSelection("../"+update) ! UpdateOthers(i+1)
					}

				// Send Requests

				case "SendRequests" =>
					println("got send requests for " + myID)
					for(i <- 0 until numOfRequests){
						var key: BigInt = sha1(getfilename())
						self ! SearchFile(key,myID,0)
					}

				case SearchFile(key: BigInt, sender: String, hopcount: Int) =>
					if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
						context.actorSelection("../"+sender) ! FoundFile(fingertable(0)(1),key,hopcount)
					} else {
						context.actorSelection("../"+fingertable(0)(1)) ! SearchFile(key,sender,hopcount+1)
					}		
				case FoundFile(owner: String, key: BigInt, hopcount: Int)=>
					println("sender = " + myID + " owner = " +owner+ " hops = " + hopcount)
			}
		}

		def getfilename(): String = {
			var i = Random.nextInt(100000)
			var str = "sdarsha".concat(Integer.toString(i,36))
			return str
		}

		def convert2Hashvalue(id: String, i:Int): String = {
			var key: BigInt = (sha1(id) + BigInt(((pow(2,i)).toInt).toString))
			key = key.mod(BigInt(((pow(2,i)).toInt).toString))
			key.toString
		}

		def sha1(s: String): BigInt = {
			val md = MessageDigest.getInstance("SHA-1")
			val bytes: Array[Byte] = md.digest(s.getBytes)
			var sb: StringBuffer = new StringBuffer
			var bi: BigInt = new BigInt(new java.math.BigInteger(bytes))
			return bi
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



		if(args.size!=2){
			println("Enter valid number of inputs!!")
		} else {
			val system = ActorSystem("MasterSystem")
			val master = system.actorOf(Props(new Master(args(0).toInt, args(1).toInt)), name = "master")
			master ! Initialize()
		}

	}	
}