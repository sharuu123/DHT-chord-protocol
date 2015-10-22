import akka.actor._
import scala.util.Random
import scala.concurrent.duration._
import scala.math.BigInt
import scala.math.pow
import java.security.MessageDigest

object project3 {
	def main(args: Array[String]){
		var big1: BigInt = BigInt("1")

		case class AddNextNode(sender: String)
		case class FindAverageHop(hopcount: Int)

		if(args.size!=2){
			println("Enter valid number of inputs!!")
		} else {
			val system = ActorSystem("MasterSystem")
			val master = system.actorOf(Props(new Master(args(0).toInt, args(1).toInt)), name = "master")
			master ! "Start"
		}
		
		println("project3 - Chord DHT")
		class Master(numOfNodes: Int, numOfRequests: Int) extends Actor{

			var count: Int =_
			var totalRequests: Int =_
			var totalHopCount: Int =_

			def receive = {
				case "Start" =>
					var friend: String = "start"
					var myID: String = "ip0"
					println("Joining nodes to the network!!")
					// println("Creating first node with ip = " +myID+" "+ sha1(myID))
					val act = context.actorOf(Props(new Node(myID, numOfRequests, friend, self)),name=myID)
					act ! "Join"  

				case AddNextNode(sender: String) =>
					print(".")
					count += 1
					if(count < numOfNodes) {
						var myID: String = "ip".concat(count.toString)
						// println("Creating node with ip = " +myID+" "+ sha1(myID))
						val act = context.actorOf(Props(new Node(myID, numOfRequests, sender, self)),name=myID)
						act ! "Join"
					} else {
						println(" ")
						println("Done with adding nodes to the network!!")
						// context.system.shutdown()
						count = 0
						totalRequests = numOfNodes*numOfRequests
						println("Nodes sending requests")
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
						context.system.shutdown()
					}
			}
		}

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

			def closestPrecedingFinger(key: BigInt): String = {
				for(i <- 0 until 160){
					var finger: BigInt = sha1(fingertable(159-i)(1))
					if(sha1(myID) < key){
						if(finger > sha1(myID) && finger < key){
							return fingertable(159-i)(1)
						}
					} else {
						if(finger > sha1(myID) || finger < key){
							return fingertable(159-i)(1)
						}
					}
				}
				return fingertable(0)(1)
			}

			def receive = {
				case "Join" =>
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
						var key: BigInt = BigInt(convert2Hashvalue(myID,0))
						// println("FindSuccessor, myID = "+myID+" "+key)
						context.actorSelection("../" + friend) ! SearchPreAndSucc(key,myID)
					}
				case SearchPreAndSucc(key: BigInt, sender: String) =>
					// 1st find predecessor of key before finding the successor which is trivial
					// once found the predecessor
					// println("SearchPreAndSucc - " + myID + " got from " + sender)
					if((sha1(myID)) < sha1(fingertable(0)(1))){
						if(key > (sha1(myID)) && key < sha1(fingertable(0)(1))){
							context.actorSelection("../"+sender) ! FoundPreAndSucc(myID,fingertable(0)(1))
							fingertable(0)(1) = sender
							// println("myID = "+myID+" pre = "+predecessor+" succ = "+fingertable(0)(1))
						} else {
							// println("SearchPreAndSucc - " + myID + " asking " + closestPrecedingFinger(key))
							context.actorSelection("../"+closestPrecedingFinger(key)) ! SearchPreAndSucc(key,sender)
						}
					} else {
						if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
							context.actorSelection("../"+sender) ! FoundPreAndSucc(myID,fingertable(0)(1))
							fingertable(0)(1) = sender
							// println("myID = "+myID+" pre = "+predecessor+" succ = "+fingertable(0)(1))
						} else {
							// println("SearchPreAndSucc - " + myID + " asking " + closestPrecedingFinger(key))
							context.actorSelection("../"+closestPrecedingFinger(key)) ! SearchPreAndSucc(key,sender)
						}
					}

				case FoundPreAndSucc(pre: String, succ: String) =>
					// println("myID = "+myID+" pre = "+pre+" succ = "+succ)
					predecessor = pre
					fingertable(0)(1) = succ
					context.actorSelection("../" + fingertable(0)(1)) ! UpdatePredecessor(myID) 

				case UpdatePredecessor(update: String) =>
					predecessor = update
					// println("myID = "+myID+" pre = "+predecessor+" succ = "+fingertable(0)(1))
					sender ! InitFingertable(1)
				
				// Message types for initializing fingertable

				case InitFingertable(i: Int) =>
					if(i<160){
						var key: BigInt = BigInt(convert2Hashvalue(myID,i))
						if((sha1(myID)) < sha1(fingertable(0)(1))){
							if(key > (sha1(myID)) && key < sha1(fingertable(0)(1))){
								fingertable(i)(0) = convert2Hashvalue(myID,i)
								fingertable(i)(1) = fingertable(0)(1)
								self ! InitFingertable(i+1)
							} else {
								context.actorSelection("../" + friend) ! SearchSuccessor(key,i,myID)
							}
						} else {
							if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
								fingertable(i)(0) = convert2Hashvalue(myID,i)
								fingertable(i)(1) = fingertable(0)(1)
								self ! InitFingertable(i+1)
							} else {
								context.actorSelection("../" + friend) ! SearchSuccessor(key,i,myID)
							}
						}
					} else {
						self ! UpdateOthers(0)
					}

				case SearchSuccessor(key: BigInt, i: Int, sender: String) =>
					if((sha1(myID)) < sha1(fingertable(0)(1))){
						if(key > (sha1(myID)) && key < sha1(fingertable(0)(1))){
							context.actorSelection("../"+sender) ! FoundSuccessor(fingertable(0)(1),i)
						} else {
							context.actorSelection("../"+fingertable(0)(1)) ! SearchSuccessor(key,i,sender)
						}
					} else {
						if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
							context.actorSelection("../"+sender) ! FoundSuccessor(fingertable(0)(1),i)
						} else {
							context.actorSelection("../"+fingertable(0)(1)) ! SearchSuccessor(key,i,sender)
						}
					}

				case FoundSuccessor(owner: String, i:Int) =>
					fingertable(i)(0) = convert2Hashvalue(myID,i)
					fingertable(i)(1) = owner
					self ! InitFingertable(i+1)

				// Message types for updating other fingertables

				case UpdateOthers(i: Int) =>
					if(160>i){
						var key: BigInt = (sha1(myID) - (big1 << i))
						key = key.mod((big1 << 160)-big1)
						context.actorSelection("../"+fingertable(0)(1)) ! SearchPredecessor(key,i,myID)
					} else {
						masterRef ! AddNextNode(myID)
					}
					
				case SearchPredecessor(key: BigInt, i: Int, sender: String) =>
					if((sha1(myID)) < sha1(fingertable(0)(1))){
						if(key > (sha1(myID)) && key < sha1(fingertable(0)(1))){
							context.actorSelection("../"+sender) ! FoundPredecessor(myID,i)
						} else {
							context.actorSelection("../"+closestPrecedingFinger(key)) ! SearchPredecessor(key,i,sender)
						}
					} else {
						if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
							context.actorSelection("../"+sender) ! FoundPredecessor(myID,i)
						} else {
							context.actorSelection("../"+closestPrecedingFinger(key)) ! SearchPredecessor(key,i,sender)
						}
					}
					
				case FoundPredecessor(pre: String, i: Int) =>
					context.actorSelection("../"+pre) ! UpdateFingerTable(myID,i)

				case UpdateFingerTable(update: String, i: Int) =>
					if(sha1(update) > (sha1(myID)) && sha1(update) < sha1(fingertable(i)(1))){
						fingertable(i)(1) = update
						context.actorSelection("../"+predecessor) ! UpdateFingerTable(update,i)
					} else {
						context.actorSelection("../"+update) ! UpdateOthers(i+1)
					}
					
				// Send Requests

				case "SendRequests" =>
					for(i <- 0 until numOfRequests){
						var key: BigInt = sha1(getfilename())
						self ! SearchFile(key,myID,0)
					}

				case SearchFile(key: BigInt, sender: String, hopcount: Int) =>
					if((sha1(myID)) < sha1(fingertable(0)(1))){
						if(key > (sha1(myID)) && key < sha1(fingertable(0)(1))){
							context.actorSelection("../"+sender) ! FoundFile(fingertable(0)(1),key,hopcount+1)
						} else {
							context.actorSelection("../"+closestPrecedingFinger(key)) ! SearchFile(key,sender,hopcount+1)
						}
					} else {
						if(key > (sha1(myID)) || key < sha1(fingertable(0)(1))){
							context.actorSelection("../"+sender) ! FoundFile(fingertable(0)(1),key,hopcount+1)
						} else {
							context.actorSelection("../"+closestPrecedingFinger(key)) ! SearchFile(key,sender,hopcount+1)
						}
					}

				case FoundFile(owner: String, key: BigInt, hopcount: Int)=>
					// print(".")
					// println("sender = " + myID + " owner = " +owner+ " pre = "+predecessor+" succ = "+fingertable(0)(1)+ " hops = " + hopcount)
					masterRef ! FindAverageHop(hopcount)
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
			key = key.mod((big1 << 160)-big1)
			key.toString
		}

		def sha1(s: String): BigInt = {
			val md = MessageDigest.getInstance("SHA-1")
			val bytes: Array[Byte] = md.digest(s.getBytes)
			var sb: StringBuffer = new StringBuffer
			var bi: BigInt = new BigInt(new java.math.BigInteger(1,bytes))
			return bi
		}

	}	
}
