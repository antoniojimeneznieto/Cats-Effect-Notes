package cheatsheet

import cats.effect.{Concurrent, Deferred, Fiber, IO, IOApp, MonadCancel, Outcome, Poll, Ref, Resource, Spawn}
import cats.effect.implicits.*
import cats.syntax.apply.*
import cats.syntax.parallel.*
import cats.Traverse
import cats.syntax.traverse.*
import cats.effect.kernel.Outcome
import cats.instances.list.*
import cats.effect.kernel.Outcome.{Canceled, Errored, Succeeded}
import cats.effect.std.Semaphore
import cats.effect.std.CountDownLatch
import cats.effect.std.CyclicBarrier
import cats.{Applicative, Functor, Monad}

import java.io.{File, FileReader}
import java.util.Scanner
import scala.concurrent.duration.*
import scala.io.StdIn
import scala.util.Random
import util.debug

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object Cheatsheet {

  def IOIntroduction() = {
    // IO
    val firstIO: IO[Int] = IO.pure {
      def function(): Int = 100
      function()
    }

    val secondIO: IO[Int] = IO.delay { // delay == apply
      println("Cheatsheet!")
      54
    }

    firstIO.flatMap(x => IO.delay(println(x)))

    def program1(): IO[Unit] = for {
      line1 <- IO(StdIn.readLine())
      line2 <- IO(StdIn.readLine())
      _ <- IO.delay(println(line1 + line2))
    } yield ()

    // MapN
    import cats.syntax.apply._
    (firstIO, secondIO, firstIO).mapN((x, y, z) => x + y + z)

    // AndThen
    firstIO >> secondIO // == firstIO.map(_ => secondIO)

    // As
    firstIO.as(5) // == firstIO.map(_ => 5)
    firstIO.void // == firstIO.map(_ => ()) it drops the value
  }

  def IOErrorHandling() = {
    // Create an Error
    val throwFailure: IO[Int] = IO.raiseError(new RuntimeException("Error")) // == delay

    // Handle exception
    throwFailure.handleErrorWith {
      case _: RuntimeException => IO.delay(println("I am here!"))
    }

    val failureAsEither: IO[Either[Throwable, Int]] = throwFailure.attempt
    failureAsEither.map {
      case Left(value) => IO.raiseError(value)
      case Right(value) => s"success $value"
    }
    val redeem: IO[String] = throwFailure.redeem(recover = exception => s"$exception", map = value => s"$value")
  }

  def IOParallelism() = {
    // Parallel MapN
    val firstIO: IO[Int] = IO.pure(100)
    val secondIO: IO[String] = IO.pure("Hello")
    val parallelMapN: IO[String] = (firstIO, secondIO).parMapN((x: Int, a: String) => s"value: $x, string: $a")
  }

  def IOTraversal() = {
    val workLoad: List[String] = List("I quite like CE", "Scala is great", "looking forward to some awesome stuff")
    def computeNumberOfWordsAsIO(string: String): IO[Int] = IO {
      Thread.sleep(Random.nextInt(1000))
      string.split(" ").length
    }.debug

    // Traverse, useful to convert your list in an IO that can be started as a fiber
    val listOfIOs: List[IO[Int]] = workLoad.map(computeNumberOfWordsAsIO)
    val ioOfList: IO[List[Int]] = listOfIOs.parSequence // List[IO] to IO[List]
    val iofList_v2: IO[List[Int]] = workLoad.parTraverse(computeNumberOfWordsAsIO) // IO[List] and apply map
  }

  def fibers() = {
    val firstIO: IO[Int] = IO.pure(100)

    // Start a fiber
    def runMultipleThreads(): IO[Outcome[IO, Throwable, Int]] = for {
      fib <- firstIO.start
      result <- fib.join
    } yield result
    val result: IO[Int] = runMultipleThreads().flatMap {
      case Succeeded(fa) => fa
      case Errored(e) => IO(0)
      case Canceled() => IO(0)
    }

    // Cancel
    def cancel() = {
      val task = IO("Starting") >> IO.sleep(1.second) >> IO("done")
      val taskWithCancelHandler = task.onCancel(IO("I was canceled!").void)

      for {
        fib <- taskWithCancelHandler.start
        _ <- IO.sleep(200.millis) >> IO("Canceling")
        _ <- fib.cancel
        result <- fib.join
      } yield result
    }
  }

  def resources() = {
    class Connection(url: String) {
      def open(): IO[String] = IO(s"opening connection to $url").debug
      def close(): IO[String] = IO(s"closing connection to $url").debug
    }
    // Bracket pattern
    val connectionBracket = IO(new Connection("github.com")).bracket(conn => conn.open())(conn => conn.close().void)

    // Resources <3
    val connectionResource = Resource.make(acquire = IO(new Connection("github.com")))(release = conn => conn.close().void)
    def concatenateResources(path: String) = for {
      scanner <- Resource.make(IO("opening file") >> IO(new Scanner(new FileReader(new File(path)))))(scanner => IO(scanner.close()))
      conn <- Resource.make(IO("opening file") >> IO(new Connection(scanner.nextLine())))(conn => conn.close().void)
    } yield conn

    // Guarantee
    val ioWithFinalizer = IO("some resource").debug.guarantee(IO("freeing resource").debug.void)
    val ioWithFinalizer_v2 = IO("some resource").debug.guaranteeCase {
      case Succeeded(fa) => fa.flatMap(result => IO(s"releasing resource: $result").debug).void
      case Errored(e) => IO("nothing to release").debug.void
      case Canceled() => IO("resource got canceled, releasing what's left").debug.void
    }


  }

  def racing() = {
    def runWithSleep[A](value: A, duration: FiniteDuration): IO[A] =
      (
        IO(s"starting computation: $value").debug >>
          IO.sleep(duration) >>
          IO(s"computation for $value: done") >>
          IO(value)
        ).onCancel(IO(s"computation CANCELED for $value").debug.void)
    val firstIO: IO[Int] = runWithSleep(1, 1.second)
    val secondIO: IO[String] = runWithSleep("Cats Effect", 2.second)

    // Race: run in different fibers, the loser is cancel and the winner is complete
    val races: IO[Either[Int, String]] = IO.race(firstIO, secondIO)
    races.flatMap {
      case Left(num) => IO(s"The number $num won!")
      case Right(string) => IO(s"The String $string won!")
    }

    // Race pair:
    val racePair: IO[Either[
      (Outcome[IO, Throwable, Int], Fiber[IO, Throwable, String]), // (winner result, loser fiber)
      (Fiber[IO, Throwable, Int], Outcome[IO, Throwable, String])  // (loser fiber, winner result)
    ]] = IO.racePair(firstIO, secondIO)
    racePair.flatMap {
      case Left((num, fibString)) => fibString.cancel >> IO(num)
      case Right(fibNum, string) => fibNum.cancel >> IO(string)
    }
  }

  def cancelling() = {
    // Manual cancellation
    IO("Waiting") >> IO.canceled >> IO(100)

    // Uncancelable
    val cancelableIO = for {
      fib <- (IO("Starting") >> IO.sleep(1.second) >> IO(100)).start
      _ <- IO.sleep(100.millis) >> fib.cancel
      _ <- fib.join
    } yield ()
    val uncancelableIO = cancelableIO.uncancelable

    // More complex example using poll
    val inputPassword = IO("Input password:").debug >> IO("(typing password)").debug >> IO.sleep(2.seconds) >> IO("RockTheJVM1!")
    val verifyPassword = (pw: String) => IO("verifying...").debug >> IO.sleep(2.seconds) >> IO(pw == "RockTheJVM1!")

    val authFlow: IO[Unit] = IO.uncancelable { poll =>
      for {
        pw <- poll(inputPassword).onCancel(IO("Authentication timed out. Try again later.").debug.void) // this is cancelable because we wrapped the IO on poll()
        verified <- verifyPassword(pw) // this is NOT cancelable
        _ <- if (verified) IO("Authentication successful.").debug // this is NOT cancelable
        else IO("Authentication failed.").debug
      } yield ()
    }

  }

  def blocking() = {
    // really blocking IOs
    val aBlockingIO = IO.blocking {
      Thread.sleep(1000)
      println(s"[${Thread.currentThread().getName}] computed a blocking code")
      100
    } // will evaluate on a thread from ANOTHER thread pool specific for blocking calls

    val iosOnManyThreads = for {
      _ <- IO("first").debug
      _ <- IO.cede // a signal to yield control over the thread - equivalent to IO.shift from CE2
      _ <- IO("second").debug // the rest of this effect may run on another thread (not necessarily)
      _ <- IO.cede
      _ <- IO("third").debug
    } yield ()

  }

  def async() = {
    // IOs can run asynchronously on fibers, without having to manually manage the fiber lifecycle
    val threadPool = Executors.newFixedThreadPool(8)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)
    type Callback[A] = Either[Throwable, A] => Unit

    def computation(): Either[Throwable, Int] = Try {
      Thread.sleep(1000)
      println(s"[${Thread.currentThread().getName}] computing the meaning of life on some other thread...")
      100
    }.toEither

    def computeOnThreadPool(): Unit = threadPool.execute(() => computation())

    // Lift the computation to an IO:
    val asyncIO: IO[Int] = IO.async_ { (cb: Callback[Int]) => // CE thread blocks (semantically) until this cb is invoked (by some other thread)
      threadPool.execute { () => // computation not managed by CE
        val result = computation()
        cb(computation()) // CE thread is notified with the result
      }
    }

    // Lift an async computation (Future) to an IO:
    def convertFutureToIO[A](future: => Future[A]): IO[A] =
      IO.async_ { (cb: Callback[A]) =>
        future.onComplete { tryResult =>
          val result = tryResult.toEither
          cb(result)
        }
      }
    lazy val aFuture: Future[Int] = Future {
      Thread.sleep(1000)
      println(s"[${Thread.currentThread().getName}] computing the meaning of life on some other thread...")
      42
    }
    val asyncIOFromFuture: IO[Int] = convertFutureToIO(aFuture)
    val asyncIOFromFuture_v2: IO[Int] = IO.fromFuture(IO(aFuture))

    // Never ending IO:
    val neverEndingIO: IO[Int] = IO.async_[Int](_ => ()) // no callback, no finish
    val neverEndingIO_v2: IO[Int] = IO.never

    // FULL ASYNC Call
    def demoAsyncCancellation() = {
      val asyncMeaningOfLifeIO_v2: IO[Int] = IO.async { (cb: Callback[Int]) => // async block ce thread until it receives the callback
        /*
          finalizer in case computation gets cancelled.
          finalizers are of type IO[Unit]
          not specifying finalizer => Option[IO[Unit]]
          creating option is an effect => IO[Option[IO[Unit]]]
         */
        // return IO[Option[IO[Unit]]]
        IO {
          threadPool.execute { () =>
            val result = computation()
            cb(result)
          }
        }.as(Some(IO("Cancelled!").debug.void))
      }

      for {
        fib <- asyncMeaningOfLifeIO_v2.start
        _ <- IO.sleep(500.millis) >> IO("cancelling...").debug >> fib.cancel
        _ <- fib.join
      } yield ()
    }

  }

  def refs() = {
    // Create a ref
    val atomicNum: IO[Ref[IO, Int]] = IO.ref(100) // == Ref[IO].of(100)

    // Play with a ref
    val modifiedNum: IO[Unit] = atomicNum.flatMap { ref => ref.set(43) } // Modify
    val getNum: IO[Int] = atomicNum.flatMap { ref => ref.get } // get
    val gsNum: IO[Int] = atomicNum.flatMap { ref => ref.getAndSet(100) }
    val updatedNum: IO[Unit] = atomicNum.flatMap { ref => ref.update(value => value * 10) }
    val ugNum: IO[Int] = atomicNum.flatMap { ref => ref.updateAndGet(value => value * 10) }
    val modifiedDifferentType: IO[String] = atomicNum.flatMap { ref => ref.modify(value => (value * 10, s"my value is $value")) }

    // Counting words with Ref
    def demoConcurrentWorkPure(): IO[Unit] = {
      def task(workload: String, total: Ref[IO, Int]): IO[Unit] = {
        val wordCount = workload.split(" ").length

        for {
          _ <- IO(s"Counting words for '$workload': $wordCount'").debug
          newCount <- total.updateAndGet(currentCount => currentCount + wordCount)
          _ <- IO(s"New total: $newCount").debug
        } yield ()
      }

      for {
        initialCount <- Ref[IO].of(0)
        _ <- List("I love Cats Effect", "This ref thing is useless", "Daniel writes a lot of code")
          .map(string => task(string, initialCount))
          .parSequence
      } yield ()
    }
  }

  def defers() = {
    // Create a defer
    val deferred: IO[Deferred[IO, Int]] = IO.deferred[Int] // == Deferred[IO, Int]

    // Typical structure: producer & consumer
    def consumer(signal: Deferred[IO, Int]) = for {
      _ <- IO("Waiting").debug
      result <- signal.get // block until getting a complete signal
      _ <- IO(s"Got the result: $result").debug
    } yield ()
    def producer(signal: Deferred[IO, Int]) = for {
      _ <- IO("Producing").debug
      _ <- IO.sleep(1.second)
      result <- IO(42)
      _ <- signal.complete(result)
    } yield ()
    def smallProgramUsingProducerConsumer(): IO[Unit] = {
      for {
        signal <- IO.deferred[Int]
        fibConsumer <- consumer(signal).start
        fibProducer <- producer(signal).start
        _ <- fibProducer.join
        _ <- fibConsumer.join
      } yield ()
    }

    // Example: Simulating downloading some content
    def fileNotifierWithDeferred(): IO[Unit] = {
      val fileParts = List("I ", "love Cats ", "Effect and", " FS2 <EOF>")

      def notifyFileComplete(signal: Deferred[IO, String]): IO[Unit] = for {
        _ <- IO("Downloading...").debug
        _ <- signal.get // block until it gets a complete signal
        _ <- IO("Download completed").debug
      } yield ()

      def downloadFilePart(part: String, contentRef: Ref[IO, String], signal: Deferred[IO, String]): IO[Unit] = for {
        _ <- IO(s"Downloading part: $part").debug
        _ <- IO.sleep(1.second)
        latestContent <- contentRef.updateAndGet(currentContent => currentContent + part)
        _ <- if (latestContent.contains("<EOF>")) signal.complete(latestContent) else IO.unit
      } yield ()

      for {
        contentRef <- IO.ref("")
        signal <- IO.deferred[String]
        notifierFib <- notifyFileComplete(signal).start
        downloadFib <- fileParts.map(part => downloadFilePart(part, contentRef, signal)).sequence.start
      //downloadFib <- fileParts.map(part => downloadFilePart(part, contentRef, signal)).parSequence.start // == compute each part in parallel and merge final result
      //downloadFib <- fileParts.traverse(part => downloadFilePart(part, contentRef, signal)).start
      //downloadFib <- fileParts.parTraverse(part => downloadFilePart(part, contentRef, signal)).start
        _ <- downloadFib.join
        _ <- notifierFib.join
      } yield ()
    }
  }

  def mutex() = {


  }

  def semaphores() = {
    // Create a Semaphore
    val semaphore: IO[Semaphore[IO]] = Semaphore[IO](2) // 2 total permits

    // Basic example using Semaphore
    def demoUsingSemaphore() = {
      def doWorkWhileLoggedIn(): IO[Int] = IO.sleep(1.second) >> IO(Random.nextInt(100))
      def weightedLogin(id: Int, requiredPermits: Int, sem: Semaphore[IO]): IO[Int] = for {
        _ <- IO(s"[session $id] waiting to log in...").debug
        _ <- sem.acquireN(requiredPermits) // acquire N permits
        // critical section
        _ <- IO(s"[session $id] logged in, working...").debug
        res <- doWorkWhileLoggedIn()
        _ <- IO(s"[session $id] done: $res, logging out...").debug
        // end of critical section
        _ <- sem.releaseN(requiredPermits) // Release N permits
      } yield res

      for {
        sem <- Semaphore[IO](2)
        user1Fib <- weightedLogin(1, 1, sem).start
        user2Fib <- weightedLogin(1, 2, sem).start
        user3Fib <- weightedLogin(1, 1, sem).start
        _ <- user1Fib.join
        _ <- user2Fib.join
        _ <- user3Fib.join
      } yield ()
    }

    // Parallel Semaphore
    def demoUsingSemaphoreInParallel() = {
      def doWorkWhileLoggedIn(): IO[Int] = IO.sleep(1.second) >> IO(Random.nextInt(100))
      val mutex = Semaphore[IO](1)

      val numbers = mutex.flatMap { sem => // otherwise we just create a new Semaphore for each fiber
        (1 to 10).toList.parTraverse { id =>
          for {
            _ <- IO(s"[session $id] waiting to log in...").debug
            _ <- sem.acquire
            // critical section
            _ <- IO(s"[session $id] logged in, working...").debug
            res <- doWorkWhileLoggedIn()
            _ <- IO(s"[session $id] done: $res, logging out...").debug
            // end of critical section
            _ <- sem.release
          } yield res
        }
      }
    }
  }

  def countdownlatches() = {
    // Basic example
    def runnerExampleForCountdownLatcher() = {
      def announcer(latch: CountDownLatch[IO]): IO[Unit] = for {
        _ <- IO("Starting race shortly...").debug >> IO.sleep(2.seconds)
        _ <- IO("5...").debug >> IO.sleep(1.second)
        _ <- latch.release
        _ <- IO("4...").debug >> IO.sleep(1.second)
        _ <- latch.release
        _ <- IO("3...").debug >> IO.sleep(1.second)
        _ <- latch.release
        _ <- IO("2...").debug >> IO.sleep(1.second)
        _ <- latch.release
        _ <- IO("1...").debug >> IO.sleep(1.second)
        _ <- IO("GO GO GO!").debug
        _ <- latch.release // gun firing
      } yield ()

      def createRunner(id: Int, latch: CountDownLatch[IO]): IO[Unit] = for {
        _ <- IO(s"[runner $id] waiting for signal...").debug
        _ <- latch.await // block this fiber until the count reaches 0
        _ <- IO(s"[runner $id] RUNNING!").debug
      } yield ()

      def sprint(): IO[Unit] = for {
        latch <- CountDownLatch[IO](5)
        announcerFib <- announcer(latch).start
        _ <- (1 to 10).toList.parTraverse(id => createRunner(id, latch))
        _ <- announcerFib.join
      } yield ()

    }


  }

  def cyclicbarriers() = {
    // Basic Example: signing up for waitlist, it is needed 10 person to launch each time
    def barriersExample() = {
      def createUser(id: Int, barrier: CyclicBarrier[IO]): IO[Unit] = for {
        _ <- IO.sleep((Random.nextDouble * 500).toInt.millis)
        _ <- IO(s"[user $id] Just heard there's a new social network - signing up for the waitlist...").debug
        _ <- IO.sleep((Random.nextDouble * 1500).toInt.millis)
        _ <- IO(s"[user $id] On the waitlist now, can't wait!").debug
        _ <- barrier.await // block the fiber when there are exactly N users waiting
        _ <- IO(s"[user $id] OMG this is so cool!").debug
      } yield ()

      def openNetwork(): IO[Unit] = for {
        _ <- IO("[announcer] The social network is up for registration! Launching when we have 10 users!").debug
        barrier <- CyclicBarrier[IO](10)
        _ <- (1 to 20).toList.parTraverse(id => createUser(id, barrier))
      } yield ()
    }
  }

  def polymorphicCancellation() = {
    //MonadCancel extends from MonadError which extends from Monad and ApplicativeError which extends Applicative
    trait MyApplicativeError[F[_], E] extends Applicative[F] {
      def raiseError[A](error: E): F[A]
      def handleErrorWith[A](fa: F[A])(f: E => F[A]): F[A]
    }
    trait MyMonadError[F[_], E] extends MyApplicativeError[F, E] with Monad[F]
    trait MyMonadCancel[F[_], E] extends MyMonadError[F, E] {
      def canceled: F[Unit]
      def uncancelable[A](poll: Poll[F] => F[A]): F[A]
    }

    import cats.syntax.flatMap._ // flatMap
    import cats.syntax.functor._ // map

    // MonadCancel describes the capability to cancel & prevent cancellation
    val monadCancelIO: MonadCancel[IO, Throwable] = MonadCancel[IO]

    // We can create values, because MonadCancel is a Monad
    val molIO: IO[Int] = monadCancelIO.pure(100)
    val ambitiousMolIO: IO[Int] = monadCancelIO.map(molIO)(_ * 10)

    val mustCompute = monadCancelIO.uncancelable { _ =>
      for {
        _ <- monadCancelIO.pure("once started...")
        res <- monadCancelIO.pure(56)
      } yield res
    }

    // goal: can generalize code
    def mustComputeGeneral[F[_], E](using mc: MonadCancel[F, E]): F[Int] = mc.uncancelable { _ =>
      for {
        _ <- mc.pure("once started, I can't go back...")
        res <- mc.pure(56)
      } yield res
    }

    // allow cancellation listeners
    val mustComputeWithListener = mustCompute.onCancel(IO("I'm being cancelled!").void)
    val mustComputeWithListener_v2 = monadCancelIO.onCancel(mustCompute, IO("I'm being cancelled!").void) // same
    import cats.effect.syntax.monadCancel._ // .onCancel

    val mustCompute_v2 = mustComputeGeneral[IO, Throwable]
    mustCompute_v2.onCancel(IO("I was cancelled!").void)

    // allow finalizers: guarantee, guaranteeCase
    val aComputationWithFinalizers = monadCancelIO.guaranteeCase(IO(42)) {
      case Succeeded(fa) => fa.flatMap(a => IO(s"successful: $a").void)
      case Errored(e) => IO(s"failed: $e").void
      case Canceled() => IO("canceled").void
    }

    // bracket pattern is specific to MonadCancel
    // therefore Resources can only be built in the presence of a MonadCancel instance
    val aComputationWithUsage = monadCancelIO.bracket(IO(42)) { value =>
      IO(s"Using the meaning of life: $value")
    } { value =>
      IO("releasing the meaning of life...").void
    }
  }

  def polymorphicFibers() = {
    // Spawn = create fibers for any effect
    trait MyGenSpawn[F[_], E] extends MonadCancel[F, E] {
      def start[A](fa: F[A]): F[Fiber[F, Throwable, A]] // creates a fiber
      def never[A]: F[A] // a forever-suspending effect
      def cede: F[Unit] // a "yield" effect

      def racePair[A, B](fa: F[A], fb: F[B]): F[Either[ // fundamental racing
        (Outcome[F, E, A], Fiber[F, E, B]),
        (Fiber[F, E, A], Outcome[F, E, B])
      ]]
    }
    trait MySpawn[F[_]] extends MyGenSpawn[F, Throwable]

    import cats.effect.syntax.spawn._ // start extension method
    import cats.syntax.functor._ // map
    import cats.syntax.flatMap._ // flatMap

    // capabilities: pure, map/flatMap, raiseError, uncancelable, start
    val spawnIO = Spawn[IO] // fetch the given/implicit Spawn[IO]
    def ioOnSomeThread[A](io: IO[A]): IO[Outcome[IO, Throwable, A]] = for {
      fib <- spawnIO.start(io) // io.start assumes the presence of a Spawn[IO]
      result <- fib.join
    } yield result

    // We generalize:
    def effectOnSomeThread[F[_], A](fa: F[A])(implicit spawn: Spawn[F]): F[Outcome[F, Throwable, A]] = for {
      fib <- fa.start
      result <- fib.join
    } yield result

    val myIO = IO.pure(100)
    val molOnFiber = ioOnSomeThread(myIO)
    val molOnFiber_v2 = effectOnSomeThread(myIO) // same

  }

  def polymorphicCoordination() = {
    // Concurrent - Ref + Deferred for ANY effect type
    trait MyConcurrent[F[_]] extends Spawn[F] {
      def ref[A](a: A): F[Ref[F, A]]
      def deferred[A]: F[Deferred[F, A]]
    }

    val concurrentIO = Concurrent[IO] // given instance of Concurrent[IO]
    val aDeferred = Deferred[IO, Int] // given/implicit Concurrent[IO] in scope
    val aDeferred_v2 = concurrentIO.deferred[Int]
    val aRef = concurrentIO.ref(42)

    // capabilities: pure, map/flatMap, raiseError, uncancelable, start (fibers), + ref/deferred

    import cats.syntax.flatMap._ // flatMap
    import cats.syntax.functor._ // map
    import cats.effect.syntax.spawn._ // start extension method

    import cheatsheet.util._
    import scala.concurrent.duration._

    def polymorphicEggBoiler[F[_]](using concurrent: Concurrent[F]): F[Unit] = {
      def unsafeSleepDupe[F[_], E](duration: FiniteDuration)(using mc: MonadCancel[F, E]): F[Unit] =
        mc.pure(Thread.sleep(duration.toMillis))
        
      def eggReadyNotification(signal: Deferred[F, Unit]) = for {
        _ <- concurrent.pure("Egg boiling on some other fiber, waiting...").debug
        _ <- signal.get
        _ <- concurrent.pure("EGG READY!").debug
      } yield ()

      def tickingClock(counter: Ref[F, Int], signal: Deferred[F, Unit]): F[Unit] = for {
        _ <- unsafeSleepDupe[F, Throwable](1.second)
        count <- counter.updateAndGet(_ + 1)
        _ <- concurrent.pure(count).debug
        _ <- if (count >= 10) signal.complete(()).void else tickingClock(counter, signal)
      } yield ()

      for {
        counter <- concurrent.ref(0)
        signal <- concurrent.deferred[Unit]
        notificationFib <- eggReadyNotification(signal).start
        clock <- tickingClock(counter, signal).start
        _ <- notificationFib.join
        _ <- clock.join
      } yield ()
    }

  }



}

object main extends IOApp.Simple {
  override def run: IO[Unit] = Cheatsheet.IOIntroduction()
}

object util {
  extension [A](io: IO[A])
    def debug: IO[A] = for {
      a <- io
      t = Thread.currentThread().getName
      _ = println(s"[$t] $a")
    } yield a

  import cats.Functor
  import cats.syntax.functor.*
  extension [F[_], A](fa: F[A]) {
    def debug(using functor: Functor[F]): F[A] = fa.map { a =>
      val t = Thread.currentThread().getName
      println(s"[$t] $a")
      a
    }
  }
}


