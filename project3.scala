import akka.actor._
import scala.util.Random
import scala.concurrent.duration._
import scala.math.BigInt
import scala.math.pow
import java.security.MessageDigest

object project3 {
	def main(args: Array[String]){
		var big1: BigInt = BigInt("1")
		var numOfFingers: Int = 160 // We are using SHA-1 for hashing(160 bits)

		case class AddNextNode(sender: String)
		case class FindAverageHop(hopcount: Int)
		case class UpdatePredecessor(update: String)
		case class InitFingertable(i: Int)
		case class SearchSuccessor(key: BigInt, i: Int, sender: String, hopcount: Int, msgType: Int) 
		case class FoundSuccessor(succ: String, pre: String, i:Int, hopcount: Int, msgType: Int)
		case class UpdateOthers(i: Int)
		case class UpdateFingerTable(update: String, i: Int)

		if(args.size!=2){
			println("Enter valid number of inputs!!")
		} else {
			println("********************************************************")
			println("project3 - Chord Distributed Hash Table")
			println("SHA-1 is used for hashing 'node ip addressess' and 'filenames'")
			println("Fingertable of each node has 160 entries")
			println("********************************************************")
			val system = ActorSystem("MasterSystem")
			val master = system.actorOf(Props(new Master(args(0).toInt, args(1).toInt)), name = "master")
			master ! "Start"
		}
		
		class Master(numOfNodes: Int, numOfRequests: Int) extends Actor{

			var count: Int =_
			var totalRequests: Int =_
			var totalHopCount: Int =_
			var printCount: Int=_

			def receive = {
				case "Start" =>
					var friend: String = "start"
					var myID: String = "ip0"
					println("Joining nodes to the network!!")
					// println("Creating first node with ip = " +myID+" "+ sha1(myID))
					val act = context.actorOf(Props(new Node(myID, numOfRequests, friend, self)),name=myID)
					act ! "Join"  

				case AddNextNode(sender: String) =>
					printCount += 1
					if(printCount == 10) {
						print(".")
						printCount = 0
					}
					count += 1
					if(count < numOfNodes) {
						var myID: String = "ip".concat(count.toString)
						val act = context.actorOf(Props(new Node(myID, numOfRequests, sender, self)),name=myID)
						act ! "Join"
					} else {
						println(" ")
						println("Done with adding nodes to the network!!")
						count = 0
						totalRequests = numOfNodes*numOfRequests
						println("sending "+numOfRequests+" requests per each node!!")
						for(i <- 0 until numOfNodes){
							var id: String = "ip".concat(i.toString)
							context.actorSelection(id) ! "SendRequests"
						}
					}

				case FindAverageHop(hopcount: Int) =>
					totalHopCount += hopcount
					count += 1 
					if(count == totalRequests){
						var averageHopCount: Int = (totalHopCount/totalRequests)
						println(" ")
						println("*** Average Number of hops = "+ averageHopCount+" ***")
						println(" ")
						context.system.shutdown()
					}
			}
		}

		class Node(myID: String, numOfRequests: Int, friend: String, masterRef:ActorRef) 
			extends Actor{

		var predecessor: String =_
		var fingertable = Array.ofDim[String](numOfFingers,2)

		def closestPrecedingFinger(key: BigInt): String = {
			for(i <- 0 until numOfFingers){
				var finger: BigInt = sha1(fingertable(numOfFingers-1-i)(1))
				if(sha1(myID) < key){
					if(finger > sha1(myID) && finger < key) return fingertable(numOfFingers-1-i)(1)
				} else {
					if(finger > sha1(myID) || finger < key) return fingertable(numOfFingers-1-i)(1)
				}
			}
			return myID
		}

		def receive = {
			case "Join" =>
				if(friend == "start") {
					predecessor = myID
					for(i <- 0 until numOfFingers){
						fingertable(i)(0) = convert2Hashvalue(myID,i)
						fingertable(i)(1) = myID
					}
					sender ! AddNextNode(myID)
				} else {
					// println("SearchPreAndSucc - " + myID + " asking " + friend)
					var key: BigInt = BigInt(convert2Hashvalue(myID,0))
					context.actorSelection("../" + friend) ! SearchSuccessor(key,0,myID,0,0)
				} 

			case UpdatePredecessor(update: String) =>
				predecessor = update
				// println("myID = "+myID+" pre = "+predecessor+" succ = "+fingertable(0)(1))
				sender ! InitFingertable(1)
			
			// Message types for initializing fingertable

			case InitFingertable(i: Int) =>
				// println("InitFingertable "+i)
				if(i<160){
					var key: BigInt = BigInt(convert2Hashvalue(myID,i))
					if((sha1(myID)) < sha1(fingertable(0)(1))){
						if(key > (sha1(myID)) && key < sha1(fingertable(0)(1))){
							fingertable(i)(0) = convert2Hashvalue(myID,i)
							fingertable(i)(1) = fingertable(0)(1)
							self ! InitFingertable(i+1)
						} else {
							context.actorSelection("../" + fingertable(0)(1)) ! SearchSuccessor(key,i,myID,0,1)
						}
					} else {
						if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
							fingertable(i)(0) = convert2Hashvalue(myID,i)
							fingertable(i)(1) = fingertable(0)(1)
							self ! InitFingertable(i+1)
						} else {
							context.actorSelection("../" + fingertable(0)(1)) ! SearchSuccessor(key,i,myID,0,1)
						}
					}
				} else {
					self ! UpdateOthers(0)
				}

			case SearchSuccessor(key: BigInt, i: Int, sender: String, hopcount: Int, msgType: Int) =>
				// println("SearchSuccessor "+myID)
				if((sha1(myID)) < sha1(fingertable(0)(1))){
					if(key > (sha1(myID)) && key < sha1(fingertable(0)(1))){
						context.actorSelection("../"+sender) ! FoundSuccessor(fingertable(0)(1),myID,i,hopcount,msgType)
						if(msgType==0) fingertable(0)(1) = sender
					} else {
						context.actorSelection("../"+closestPrecedingFinger(key)) ! SearchSuccessor(key,i,sender,hopcount+1,msgType)
					}
				} else {
					if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
						context.actorSelection("../"+sender) ! FoundSuccessor(fingertable(0)(1),myID,i,hopcount,msgType)
						if(msgType==0) fingertable(0)(1) = sender
					} else {
						context.actorSelection("../"+closestPrecedingFinger(key)) ! SearchSuccessor(key,i,sender,hopcount+1,msgType)
					}
				}

			case FoundSuccessor(succ: String, pre: String, i:Int, hopcount: Int, msgType: Int) =>
				if(msgType == 0){  // Find predecessor and Successor
					predecessor = pre
					fingertable(0)(0) = convert2Hashvalue(myID,0)
					fingertable(0)(1) = succ
					context.actorSelection("../" + fingertable(0)(1)) ! UpdatePredecessor(myID)
				} else if(msgType == 1) { // Init Fingertable
					fingertable(i)(0) = convert2Hashvalue(myID,i)
					fingertable(i)(1) = succ
					self ! InitFingertable(i+1)
				} else if(msgType == 2) { // For updating other fingertables
					context.actorSelection("../"+pre) ! UpdateFingerTable(myID,i)
				} else if(msgType == 3) {
					masterRef ! FindAverageHop(hopcount)
				}
				
			case UpdateOthers(i: Int) =>
				if(numOfFingers>i){
					var key: BigInt = (sha1(myID) - (big1 << i))
					key = key.mod((big1 << numOfFingers)-big1)
					context.actorSelection("../"+fingertable(0)(1)) ! SearchSuccessor(key,i,myID,0,2)
				} else {
					masterRef ! AddNextNode(myID)
				}

			case UpdateFingerTable(update: String, i: Int) =>
				if(sha1(update) > (sha1(myID)) && sha1(update) < sha1(fingertable(i)(1))){
					fingertable(i)(1) = update
					context.actorSelection("../"+predecessor) ! UpdateFingerTable(update,i)
				} else {
					context.actorSelection("../"+update) ! UpdateOthers(i+1)
				}
				
			case "SendRequests" =>
				for(i <- 0 until numOfRequests){
					var key: BigInt = sha1(getfilename())
					self ! SearchSuccessor(key,0,myID,0,3)
				}
		}
		}

		def getfilename(): String = {
			var i = Random.nextInt(100000)
			var str = "sdarsha".concat(Integer.toString(i,36))
			return str
		}

		def convert2Hashvalue(id: String, i:Int): String = {
			var big1: BigInt = BigInt("1")
			var key: BigInt = (sha1(id) + (big1 << i))
			key = key.mod((big1 << numOfFingers)-big1)
			key.toString
		}

		def sha1(s: String): BigInt = {
			val md = MessageDigest.getInstance("SHA-1")
			val bytes: Array[Byte] = md.digest(s.getBytes)
			var bi: BigInt = new BigInt(new java.math.BigInteger(1,bytes))
			return bi
		}
	}	
}