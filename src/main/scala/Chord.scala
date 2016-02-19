package main.scala

import akka.actor._
import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.concurrent.duration._
import scala.Char._
import scala.language.postfixOps
import java.util.concurrent.TimeUnit
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.language.postfixOps
import java.math.BigInteger

case class Askjoin(node: ActorRef, master: ActorRef)
case class createNodeDetails()
case class joinFinished(nodeFinished: ActorRef)
case class findSuccessor(identifier: BigInt)
case class getSuccessor()
case class updateFingerTable(act: ActorRef, actorId: BigInt, loopIdentifier: Int, NthpredId: BigInt)
case class callFindPredecessor(ident: BigInt)
case class updatepredecessor(node: ActorRef, id: BigInt)
case class updateSuccessor(node: ActorRef, id: BigInt)
case class sendYourFingerTable()
case class Route(Key: BigInt, numOfHops: Int, Sourcenode: BigInt, Path: String)
case class RoutingFinished(SourceNode: BigInt, nodenumofHops: Int, Lastnode: BigInt, Path: String, key: BigInt, NodeBefore: BigInt)
case class generateRequest(Key: BigInt)
case class StartRouting()

object Chord {
  def main(args: Array[String]) {

    if (args.length != 2) {
      println("No Argument(s)! Exiting:")
    } else {
      chord(numNodes = args(0).toInt, numRequests = args(1).toInt)

    }

    def chord(numNodes: Int, numRequests: Int) {
      val system = ActorSystem("Chord")
      val nodeCreater = system.actorOf(Props(new NodeCreater(numNodes, numRequests)), name = "master")
      nodeCreater ! createNodeDetails()
    }
  }
}
class FingerTable {
  var node: ActorRef = null
  var start: BigInt = 0
  var id: BigInt = 0

  def setNode(nd: ActorRef) =
    {
      node = nd
    }

  def getNode() =
    {
      node
    }

  def setStart(strt: BigInt) = {
    start = strt
  }

  def getStart(): BigInt = {
    start
  }

  def setId(nodeID: BigInt) = {

    id = nodeID

  }
  def getId(): BigInt = {
    id
  }

}
class NodeCreater(numNodes: Int, numRequests: Int) extends Actor {

  import context._

  //the number of bits
  var m = ceil((math.log(numNodes + 1) / math.log(2))).toInt
  m = 160
  println("The value of m is " + m)

  // The nodes could range from 0 to keyListSize
  val keysListSize: Int = (pow(2, m) - 1).toInt
  var randomGen: Int = 0

  var nodeListFinal = new ListBuffer[String]()
  var nodeIntheNetwork = new ListBuffer[ActorRef]()
  var TotalRequestmade: Int = 0;
  var TotalRequestRecived: Int = 0;
  var TotalnumOfHops: Int = 0;
  var fingerTableSize = (((log(m) / log(2)).toInt)/2 )+1
  var nodesInRing = new ListBuffer[BigInt]()

  println("The keys could range upto " + keysListSize)
  println("Finger table size = " + fingerTableSize)
  println("Creating the Nodes .... ")
  
  while (nodeListFinal.size < numNodes) {
    var temp: Int = abs(util.Random.nextInt(keysListSize))
    if (!(nodeListFinal.contains(temp))) {
      
      var nodeIdInRing: BigInt = Encode("node_" + temp.toString)
      if(!(nodesInRing.contains(nodeIdInRing)))
          { nodesInRing += nodeIdInRing
            nodeListFinal += "node_" + temp.toString
          }
      
    }

  }

  //creating the nodes as actors

  val Nodes = (for { i <- 0 until numNodes } yield {

    var NodeRef = context.actorOf(Props(new ChordActor(numNodes, numRequests, nodesInRing(i), m)), name = String.valueOf(nodesInRing(i)))
    (i, NodeRef)
  }).toMap

  def receive = {

    case StartRouting() =>

      for (i <- 0 until numRequests) {
      
      
        for (j <- 0 until nodeIntheNetwork.size) {
          var filenum :Int = util.Random.nextInt(nodeIntheNetwork.size)   
       
          var filetofind = filenum.toString + "File"
          var file: BigInt = Encode(filetofind)
          nodeIntheNetwork(j) ! generateRequest(file)
        }
      }
      TotalRequestmade = (numRequests * numNodes)
      println("Total Requests Made: " + TotalRequestmade)

    case RoutingFinished(sourcenode, numOfHops, lastNodeID, path, key, predecessorID) =>

      TotalRequestRecived += 1
      TotalnumOfHops += numOfHops
      println("File " + key + " found ")
      println("Source : " +sourcenode )
      println("Destination : " + lastNodeID )
      println("Hops : " + numOfHops)

      println("Routing path : " + path )
      println("Total completed requests uptil now: " + TotalRequestRecived)
      println("Total Hops until now: " + TotalnumOfHops +"\n\n")

      var Average: Float = TotalnumOfHops.toFloat / TotalRequestmade

      if (TotalRequestRecived == TotalRequestmade) {
        println("Routing Finished !")
        println("Average Number of Hops = " + Average)
        context.system.shutdown()
      }

    case createNodeDetails() =>
      var nodeToContact: ActorRef = null
      for (i <- 0 until numNodes) {
        Thread.sleep(10)
        if (nodeIntheNetwork.size > 0) {
          nodeToContact = nodeIntheNetwork(util.Random.nextInt(nodeIntheNetwork.size))

        }

        var j: Int = 0
        implicit val timeout = Timeout(10 seconds)

        val future = Nodes(i) ? Askjoin(nodeToContact, self)
        val result = Await.result(future, timeout.duration).asInstanceOf[String]

        if (result.equals("Done")) {

          println("Adding Nodes...")
          nodeIntheNetwork += Nodes(i)

        }

      }

      Thread.sleep(1000)
      println(" All Nodes have been added now")
      for (i <- 0 until numNodes) {

        implicit val timeout = Timeout(10 seconds)
        val future = Nodes(i) ? sendYourFingerTable()
        val (result, idd) = Await.result(future, timeout.duration).asInstanceOf[(ArrayBuffer[String], BigInt)]

        println("\nThe finger Table of Node" + idd + " is")
        for (i <- 0 until fingerTableSize) {
          println(result(i))
        }
        println()
      }

      //Starting to route  

      self ! StartRouting()

  }

  def Encode(s: String): BigInt = {
    var hashByte = java.security.MessageDigest.getInstance("SHA-1").digest(s.getBytes("UTF-8"))
    //var tempHash : Array[Byte] = Array(hashByte(0))
    //var encoded :BigInt = new BigInt(new BigInteger(1,tempHash)) --- If truncation required
    var encoded: BigInt = new BigInt(new BigInteger(1, hashByte))
    return encoded
  }

}

class ChordActor(numNodes: Int, numRequests: Int, nodeId: BigInt, m: Int) extends Actor {

  val nodeID = nodeId;
  var successor: ActorRef = null;
  var predecessor: ActorRef = null;
  var predecessorId: BigInt = 0
  var successorId: BigInt = 0
  var fingerTable = new ArrayBuffer[FingerTable]()
  var tracker: ActorRef = null
  var fingerTableSize = (((log(m) / log(2)).toInt)/2) +1
  val keysListSize: Int = (pow(2, m) - 1).toInt

  // Initializing the finger table
  calculateFingerTableStart()

  def receive = {
    case generateRequest(key) =>

      val numOfHops: Int = 0
      var path: String = (nodeID.toString() + " ")
      var (closestMatch, closestMatchId) = findClosestPrecedingId(key, self, nodeID, "Route").asInstanceOf[(ActorRef, BigInt)]

      if (closestMatch == self) {
        tracker ! RoutingFinished(nodeID, numOfHops, nodeID, path, key, predecessorId)
      } else {
        closestMatch ! Route(key, numOfHops, nodeID, path)
      }

    case Askjoin(arbitraryNode, master) =>

      tracker = master
      var result1: ActorRef = null
      var result2: ActorRef = null

      if (arbitraryNode == null) { //First node Joins
        for (i <- 0 until fingerTableSize) {
          fingerTable(i).setNode(context.self)
          fingerTable(i).setId(nodeID)
        }
        successor = fingerTable(0).getNode()
        predecessor = context.self
        successorId = nodeID
        predecessorId = nodeID

        sender ! "Done"

      } else {

        implicit val timeout = Timeout(10 seconds)
        val future = arbitraryNode ? findSuccessor(fingerTable(0).getStart())
        var (succ, succId, pred, predId) = Await.result(future, timeout.duration).asInstanceOf[(ActorRef, BigInt, ActorRef, BigInt)]

        fingerTable(0).setNode(succ)
        fingerTable(0).setId(succId)
        successorId = succId
        successor = succ
        predecessor = pred
        predecessorId = predId

        succ ! updatepredecessor(self, nodeID)
        pred ! updateSuccessor(self, nodeID)

        for (i <- 0 until fingerTableSize - 1) {

          var intervalEnd = fingerTable(i).getId()
          var valueToFind = fingerTable(i + 1).getStart()
          if (intervalEnd.<(nodeID)) {
            intervalEnd = intervalEnd + Math.pow(2, m).toInt
            if (valueToFind.<(nodeID)) {
              valueToFind = valueToFind + Math.pow(2, m).toInt
            }
          }

          if ((valueToFind.>=(nodeID) && valueToFind.<=(intervalEnd))) {
            fingerTable(i + 1).setNode(fingerTable(i).getNode())
            fingerTable(i + 1).setId(fingerTable(i).getId())
          } else {

            var fingerTableSucc: ActorRef = null
            implicit val timeout = Timeout(10 seconds)
            val future = arbitraryNode ? findSuccessor(fingerTable(i + 1).getStart())
            var (succ, succId, pred, predId) = Await.result(future, timeout.duration).asInstanceOf[(ActorRef, BigInt, ActorRef, BigInt)]
            fingerTable(i + 1).setNode(succ)
            fingerTable(i + 1).setId(succId)
          }

        }

        //Updating other nodes' finger tables
        updateOthers()
        successor = fingerTable(0).getNode()
        sender ! "Done"
      }

    case Route(key, numOfHops, sourcenode, path) =>

      var hops: Int = numOfHops + 1
      var newpath = path + "===>" + nodeID.toString() 
      var (closestMatch, closestMatchId) = findClosestPrecedingId(key, self, nodeID, "Route").asInstanceOf[(ActorRef, BigInt)]

      if (closestMatch == self) {
        tracker ! RoutingFinished(sourcenode, hops, nodeID, newpath, key, predecessorId)
      } else {
        closestMatch ! Route(key, hops, nodeID, newpath)
      }

    case updateFingerTable(s, sid, i, nthPredID) =>
      if (nthPredID == successorId) {
        var nTHPredID = -1
        successor ! updateFingerTable(s, sid, i, nTHPredID)
      } else {
        var intervalEnd = fingerTable(i).getId()
        var valueToFind = sid

        if (intervalEnd.<=(nodeID)) {
          intervalEnd = intervalEnd + Math.pow(2, m).toInt
          if (valueToFind.<(nodeID)) {
            valueToFind = valueToFind + Math.pow(2, m).toInt
          }
        }

        if (valueToFind.>(nodeID) && valueToFind.<(intervalEnd)) {
          var key = fingerTable(i).getStart()
          var newNode = sid
          if (newNode.<=(nodeID)) {
            newNode = newNode + Math.pow(2, m).toInt
            if (key.<(nodeID)) {
              key = key + Math.pow(2, m).toInt
            }
          }
          if (key.>(nodeID) && key.<=(newNode)) {
            fingerTable(i).setNode(s)
            fingerTable(i).setId(sid)
            if (i == 0) {
              successor = fingerTable(0).getNode()
              successorId = fingerTable(0).getId()
            }

          }

          predecessor ! updateFingerTable(s, sid, i, nthPredID)

        } else if (self == s) {
          predecessor ! updateFingerTable(s, sid, i, nthPredID)
        }
      }
    case updatepredecessor(pred, predId) =>
      predecessor = pred
      predecessorId = predId

    case updateSuccessor(succ, succId) =>
      successor = succ
      successorId = succId

    case getSuccessor() =>
      sender ! (successor, successorId)

    case callFindPredecessor(identifer: BigInt) =>
      var (predNode1, predNodeID1) = findPredecessor(identifer)
      sender ! (predNode1, predNodeID1)

    case sendYourFingerTable() =>
      var a = ArrayBuffer[String]()
      for (i <- 0 until fingerTableSize) {
        var str = "Start: " + fingerTable(i).getStart() + "  Succ: " + fingerTable(i).getId()
        a += str
      }
      sender ! (a, nodeID)

    case findSuccessor(identifer: BigInt) =>
      var succ: ActorRef = null;
      var (pred, predID) = findPredecessor(identifer)
      if (pred == self) {
        succ = successor
        sender ! (succ, successorId, pred, predID)

      } else {
        implicit val timeout = Timeout(10 seconds)
        val future = pred ? getSuccessor()
        var (succ, succId) = Await.result(future, timeout.duration).asInstanceOf[(ActorRef, BigInt)]
        sender ! (succ, succId, pred, predID)
      }
  }

  def calculateFingerTableStart() = {
    for (i <- 0 until fingerTableSize) {
      var start = nodeId + BigInteger.valueOf(Math.pow(2, i).toInt % Math.pow(2, m).toInt)
      var ft = new FingerTable()
      ft.setStart(start)
      fingerTable += ft
    }
  }
  def findPredecessor(identifer: BigInt): (ActorRef, BigInt) = {
    var predNode = self;
    var predNodeID: BigInt = nodeID
    var intervalEnd = successorId
    var valueToFind = identifer
    if (successorId.<(nodeID)) {
      intervalEnd = successorId + Math.pow(2, m).toInt
      if (identifer.<(nodeID)) {
        valueToFind = identifer + Math.pow(2, m).toInt
      }
    }
    if (!(valueToFind.>(nodeID) && valueToFind.<=(intervalEnd))) {
      var (predNode1, predNodeID1) = findClosestPrecedingId(identifer, predNode, nodeID, "Join").asInstanceOf[(ActorRef, BigInt)]

      if (!(predNode1 == self)) {
        implicit val timeout = Timeout(10 seconds)
        val future = predNode1 ? callFindPredecessor(identifer)
        var (succ, succId) = Await.result(future, timeout.duration).asInstanceOf[(ActorRef, BigInt)]
        return (succ, succId)
      } else {
        return (predNode1, predNodeID1)
      }
    }

    (predNode, predNodeID)

  }
  def findClosestPrecedingId(identifer: BigInt, predNode: ActorRef, nodeID: BigInt, callType: String): (ActorRef, BigInt) = {
    callType match {
      case "Join" =>
        if (identifer == nodeID) {
          return (predecessor, predecessorId)
        }
        for (i <- fingerTableSize - 1 to 0 by -1) {

          var intervalEnd = identifer
          var valueToFind = fingerTable(i).getStart()
          if (identifer.<=(nodeID)) {
            intervalEnd = identifer + Math.pow(2, m).toInt
            if (fingerTable(i).getId().<=(nodeID)) {
              valueToFind = fingerTable(i).getId() + Math.pow(2, m).toInt
            }
          }

          if (valueToFind.>(nodeID) && valueToFind.<(intervalEnd)) {
            return (fingerTable(i).getNode(), fingerTable(i).getId())
          }

        }
        (predNode, nodeID)

      case "Route" =>
        var keyToFind = identifer
        var myId: BigInt = nodeID
        if (predecessorId.>(nodeID)) {
          myId = nodeID + (Math.pow(2, m).toInt)
        }

        if (keyToFind.>(predecessorId) && keyToFind.<=(myId)) {
          return (self, nodeID)
        } else {
          for (i <- 0 until fingerTableSize - 1) {
            var currentFingerStartval = fingerTable(i).getStart()
            var nextFingerStartval = fingerTable(i + 1).getStart()
            var SucceedingNodeID = fingerTable(i).getId()

            //If key matches this is the value we are looking for
            if (currentFingerStartval.==(keyToFind)) {

              return (fingerTable(i).getNode(), fingerTable(i).getId())
            }
            // Taking care of modulo

            if (nextFingerStartval.<(currentFingerStartval)) {
              nextFingerStartval = nextFingerStartval + Math.pow(2, m).toInt
              if (keyToFind.<(currentFingerStartval)) {
                keyToFind = keyToFind + Math.pow(2, m).toInt
              }
            }

            if (keyToFind.>(currentFingerStartval) && keyToFind.<=(nextFingerStartval)) {
              return (fingerTable(i + 1).getNode(), fingerTable(i + 1).getId())

            }

          }
          return (fingerTable(fingerTableSize - 1).getNode(), fingerTable(fingerTableSize - 1).getId())

        }
        (predNode, nodeID)
    }

  }
  def updateOthers(): Unit = {
    for (i <- 0 until fingerTableSize) {
      var dividend = (nodeID - BigInteger.valueOf(Math.pow(2, i).toInt))
      var divisor = Math.pow(2, m).toInt

      var identifier: BigInt = (((dividend % divisor) + divisor) % divisor)

      if (identifier > keysListSize) {
        identifier = identifier - keysListSize
      }

      var (p, pid) = findPredecessor(identifier)

      if (p == self) {
        p = predecessor
        pid = predecessorId
      }

      p ! updateFingerTable(self, nodeID, i, identifier)
    }
  }

  def Encode(s: String): BigInt = {
    var hashByte = java.security.MessageDigest.getInstance("SHA-1").digest(s.getBytes("UTF-8"))
    // var tempHash: Array[Byte] = Array(hashByte(0))
    //var encoded :BigInt = new BigInt(new BigInteger(1,tempHash))
    var encoded: BigInt = new BigInt(new BigInteger(1, hashByte))
    return encoded
  }

}