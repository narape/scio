/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.extra.sparkey

import com.github.benmanes.caffeine.cache.{Cache => CCache, Caffeine}
import com.spotify.scio._
import com.spotify.scio.extra.sparkey.instances.MockStringSparkeyReader
import com.spotify.scio.testing._
import com.spotify.scio.util._
import com.spotify.scio.values.SCollection
import com.spotify.sparkey._
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files
import java.util.Arrays
import scala.jdk.CollectionConverters._

final case class TestCache[K, V](testId: String) extends CacheT[K, V, CCache[K, V]] {
  @transient private lazy val cache =
    TestCache.caches
      .get(testId, Cache.caffeine(Caffeine.newBuilder().recordStats().build()))
      .asInstanceOf[CacheT[K, V, CCache[K, V]]]

  override def get(k: K): Option[V] = cache.get(k)

  override def get(k: K, default: => V): V = cache.get(k, default)

  override def put(k: K, value: V): Unit = {
    cache.put(k, value)
    ()
  }

  override def invalidateAll(): Unit = cache.invalidateAll()

  override def underlying: CCache[K, V] = cache.underlying
}

object TestCache {
  private val caches = Cache.concurrentHashMap[String, AnyRef]

  @inline def apply[K, V](): TestCache[K, V] =
    TestCache[K, V](scala.util.Random.nextString(5))
}

object SparkeyJob {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    def flattenSparkey(r: SparkeyReader): Iterator[(String, String)] =
      r.iterator().asScala.map(e => e.getKeyAsString -> e.getValueAsString)

    // read sparkey input
    val si = sc.sparkeySideInput(args("input"))
    val kvs = sc
      .parallelize(List(1))
      .withSideInputs(si)
      .flatMap { case (_, ctx) => flattenSparkey(ctx(si)) }
      .toSCollection
    // save sparkey output
    kvs.saveAsSparkey(args("output1"))

    // use interim sparkey that is unmocked with a temp location
    val si2 = kvs.asSparkeySideInput
    sc.parallelize(List(1))
      .withSideInputs(si2)
      .flatMap { case (_, ctx) => flattenSparkey(ctx(si2)) }
      .toSCollection
      .saveAsSparkey(args("output2"))

    sc.run()
    ()
  }
}

class SparkeyTest extends PipelineSpec {
  /* We're using keys longer than a single character here to trigger edge-case behaviour
   * in MurmurHash3, which is used by ShardedSparkeyReader.
   * tl;dr:
   *   MurmurHash3.stringHash(x) == MurmurHash3.bytesHash(x.getBytes) if x.length == 1
   *   MurmurHash3.stringHash(x) != MurmurHash3.bytesHash(x.getBytes) if x.length > 1
   */
  val sideData: Seq[(String, String)] = Seq(("ab", "1"), ("bc", "2"), ("cd", "3"))
  val bigSideData: IndexedSeq[(String, String)] =
    (0 until 100).map(i => (('a' + i).toString, i.toString))

  "JobTest" should "support mocking sparkey" in {
    val input = Map("a" -> "b", "c" -> "d")
    JobTest[SparkeyJob.type]
      .args("--input=foo", "--output1=bar", "--output2=baz")
      .input(SparkeyIO("foo"), Seq(MockStringSparkeyReader(input)))
      .output(SparkeyIO.output[String, String]("bar"))(_ should containInAnyOrder(input))
      .output(SparkeyIO.output[String, String]("baz"))(_ should containInAnyOrder(input))
      .run()
  }

  "SCollection" should "support .asSparkey with temporary local file" in {
    val (_, sparkeyUris) = runWithLocalOutput(_.parallelize(sideData).asSparkey)
    val basePath = sparkeyUris.head.basePath
    val reader = Sparkey.open(new File(basePath))
    reader.toMap shouldBe sideData.toMap
    for (ext <- Seq(".spi", ".spl")) {
      new File(basePath + ext).delete()
    }
  }

  it should "support reading in an existing Sparkey file" in {
    // Create a temporary Sparkey file pair
    val (_, sparkeyUris) = runWithLocalOutput(_.parallelize(sideData).asSparkey)
    val basePath = sparkeyUris.head.basePath

    val (_, result) = runWithLocalOutput { sc =>
      val sparkey = sc.sparkeySideInput(basePath)
      sc
        .parallelize(Seq(1))
        .withSideInputs(sparkey)
        .flatMap { case (_, sic) => sic(sparkey).toList }
        .toSCollection
    }
    result.toSet shouldBe sideData.toSet

    for (ext <- Seq(".spi", ".spl")) {
      new File(basePath + ext).delete()
    }
  }

  it should "support reading in an existing sharded Sparkey collection" in {
    // Create a temporary Sparkey file pair
    val (_, sparkeyUris) = runWithLocalOutput(_.parallelize(sideData).asSparkey(numShards = 2))
    val sparkeyUri = sparkeyUris.head
    val globExpression = sparkeyUri.globExpression

    val (_, result) = runWithLocalOutput { sc =>
      val sparkey = sc.sparkeySideInput(globExpression)
      sc
        .parallelize(Seq(1))
        .withSideInputs(sparkey)
        .flatMap { case (_, sic) => sic(sparkey).toList }
        .toSCollection
    }
    result.toSet shouldBe sideData.toSet

    FileUtils.deleteDirectory(new File(sparkeyUri.basePath))
  }

  it should "support .asSparkey with shards" in {
    val (_, sparkeyUris) = runWithLocalOutput(_.parallelize(bigSideData).asSparkey(numShards = 2))
    val sparkeyUri = sparkeyUris.head

    val allSparkeyFiles = FileSystems
      .`match`(sparkeyUri.globExpression)
      .metadata
      .asScala
      .map(_.resourceId.toString)

    val basePaths = allSparkeyFiles.map(_.replaceAll("\\.sp[il]$", "")).toSet
    basePaths.size shouldBe 2

    val readers = basePaths.map(basePath => Sparkey.open(new File(basePath)))
    readers.map(_.toMap.toList.toMap).reduce(_ ++ _) shouldBe bigSideData.toMap

    val rfu = RemoteFileUtil.create(PipelineOptionsFactory.create())
    val shardedReader = sparkeyUri.getReader(rfu)
    shardedReader.toMap shouldBe bigSideData.toMap

    bigSideData.foreach { case (expectedKey, expectedValue) =>
      shardedReader.get(expectedKey) shouldBe Some(expectedValue)
    }

    FileUtils.deleteDirectory(new File(sparkeyUri.basePath))
  }

  it should "write empty shards when no data" in {
    val (_, sparkeyUris) =
      runWithLocalOutput(_.parallelize(Seq.empty[(String, String)]).asSparkey(numShards = 2))
    val sparkeyUri = sparkeyUris.head

    val allSparkeyFiles = FileSystems
      .`match`(sparkeyUri.globExpression)
      .metadata
      .asScala
      .map(_.resourceId.toString)

    val basePaths = allSparkeyFiles.map(_.replaceAll("\\.sp[il]$", "")).toSet
    basePaths.size shouldBe 2

    val readers = basePaths.map(basePath => Sparkey.open(new File(basePath)))
    readers.flatMap(_.iterator().asScala).size shouldBe 0

    FileUtils.deleteDirectory(new File(sparkeyUri.basePath))
  }

  it should "support .asSparkey with specified local file" in {
    val tmpDir = Files.createTempDirectory("sparkey-test-")
    val basePath = tmpDir.resolve("sparkey").toString
    runWithRealContext()(_.parallelize(sideData).asSparkey(basePath)).waitUntilDone()
    val reader = Sparkey.open(new File(basePath + ".spi"))
    reader.toMap shouldBe sideData.toMap
    for (ext <- Seq(".spi", ".spl")) {
      new File(basePath + ext).delete()
    }
  }

  it should "support .asSparkey with specified local file and shards" in {
    val tmpDir = Files.createTempDirectory("sparkey-test-")
    val basePath = tmpDir.resolve("new-sharded")
    Files.createDirectory(basePath)
    runWithRealContext()(_.parallelize(bigSideData).asSparkey(basePath.toString, numShards = 2))
      .waitUntilDone()

    val allSparkeyFiles = FileSystems
      .`match`(s"$basePath/part-*")
      .metadata
      .asScala
      .map(_.resourceId.toString)

    val basePaths = allSparkeyFiles.map(_.replaceAll("\\.sp[il]$", "")).toSet
    basePaths.size shouldBe 2

    val readers = basePaths.map(basePath => Sparkey.open(new File(basePath)))
    readers.map(_.toMap.toList.toMap).reduce(_ ++ _) shouldBe bigSideData.toMap

    for (ext <- allSparkeyFiles) {
      new File(s"$basePath$ext").delete()
    }
  }

  it should "throw exception when Sparkey file exists" in {
    val tmpDir = Files.createTempDirectory("sparkey-test-")
    val basePath = tmpDir.resolve("sparkey").toString
    val index = new File(basePath + ".spi")
    Files.createFile(index.toPath)

    the[IllegalArgumentException] thrownBy {
      runWithRealContext()(_.parallelize(sideData).asSparkey(basePath)).waitUntilDone()
    } should have message s"requirement failed: Sparkey URI $basePath already exists"

    index.delete()
  }

  it should "support .asSparkey with Array[Byte] key, value" in {
    val tmpDir = Files.createTempDirectory("sparkey-test-")
    val sideDataBytes = sideData.map(kv => (kv._1.getBytes, kv._2.getBytes))
    val basePath = s"$tmpDir/my-sparkey-file"
    runWithRealContext()(_.parallelize(sideDataBytes).asSparkey(basePath)).waitUntilDone()
    val reader = Sparkey.open(new File(basePath + ".spi"))
    sideDataBytes.foreach(kv => Arrays.equals(reader.getAsByteArray(kv._1), kv._2) shouldBe true)
    for (ext <- Seq(".spi", ".spl")) {
      new File(basePath + ext).delete()
    }
  }

  it should "support .asSparkeySideInput" in {
    val input = Seq("ab", "bc", "ab", "bc")
    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey = sc.parallelize(sideData).asSparkey
      val si = sparkey.asSparkeySideInput
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .map((x, sic) => sic(si)(x))
        .toSCollection
      (sparkey, result)
    }

    result.toList.sorted shouldBe input.map(sideData.toMap).sorted

    val basePath = sparkeyUris.head.basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support .asSparkeySideInput with shards" in {
    val input = Seq("ab", "bc", "ab", "bc")
    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey = sc.parallelize(sideData).asSparkey(numShards = 10)
      val si = sparkey.asSparkeySideInput
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .map((x, sic) => sic(si)(x))
        .toSCollection
      (sparkey, result)
    }

    val rfu = RemoteFileUtil.create(PipelineOptionsFactory.create())
    result.toList.sorted shouldBe input.map(sideData.toMap).sorted
    val sparkeyUri = sparkeyUris.head
    val shardedReader = sparkeyUri.getReader(rfu)
    sideData.foreach { case (expectedKey, expectedValue) =>
      shardedReader.get(expectedKey) shouldBe Some(expectedValue)
    }

    FileUtils.deleteDirectory(new File(sparkeyUri.basePath))
  }

  it should "support .asSparkeySideInput with shards and missing values" in {
    val input = Seq("ab", "bc", "ab", "bc", "de", "ef")
    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey = sc.parallelize(sideData).asSparkey(numShards = 10)
      val si = sparkey.asSparkeySideInput
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .flatMap((x, sic) => sic(si).get(x))
        .toSCollection
      (sparkey, result)
    }
    val sideDataMap = sideData.toMap
    result.toList.sorted shouldBe input.flatMap(sideDataMap.get).sorted

    val rfu = RemoteFileUtil.create(PipelineOptionsFactory.create())
    val sparkeyUri = sparkeyUris.head
    val shardedReader = sparkeyUri.getReader(rfu)
    sideData.foreach { case (expectedKey, expectedValue) =>
      shardedReader.get(expectedKey) shouldBe Some(expectedValue)
    }

    FileUtils.deleteDirectory(new File(sparkeyUri.basePath))
  }

  it should "support .asCachedStringSparkeySideInput" in {
    val input = Seq("ab", "bc", "ab", "bc")
    val cache = TestCache[String, String]()
    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey = sc.parallelize(sideData).asSparkey
      val si = sparkey.asCachedStringSparkeySideInput(cache)
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .map((x, sic) => sic(si)(x))
        .toSCollection
      (sparkey, result)
    }
    result.toList.sorted shouldBe input.map(sideData.toMap).sorted

    cache.underlying.stats().requestCount shouldBe input.size
    cache.underlying.stats().loadCount shouldBe input.toSet.size

    val basePath = sparkeyUris.head.basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support .asCachedStringSparkeySideInput with shards" in {
    val input = Seq("ab", "bc", "ab", "bc")
    val cache = TestCache[String, String]()

    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey = sc.parallelize(sideData).asSparkey(numShards = 2)
      val si = sparkey.asCachedStringSparkeySideInput(cache)
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .map((x, sic) => sic(si)(x))
        .toSCollection
      (sparkey, result)
    }
    result.toList.sorted shouldBe input.map(sideData.toMap).sorted

    cache.underlying.stats().requestCount shouldBe input.size
    cache.underlying.stats().loadCount shouldBe input.toSet.size

    val basePath = sparkeyUris.head.basePath
    FileUtils.deleteDirectory(new File(basePath))
  }

  it should "support .asTypedSparkeySideInput" in {
    val input = Seq("ab", "bc", "cd", "de")
    val typedSideData = Seq(("ab", Seq(1, 2)), ("bc", Seq(2, 3)), ("cd", Seq(3, 4)))
    val typedSideDataMap = typedSideData.toMap

    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey =
        sc.parallelize(typedSideData).mapValues(_.map(_.toString).mkString(",")).asSparkey
      val si = sparkey.asTypedSparkeySideInput[Seq[Int]] { b: Array[Byte] =>
        new String(b).split(",").toSeq.map(_.toInt)
      }
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .flatMap((x, sic) => sic(si).get(x))
        .toSCollection
      (sparkey, result)
    }
    val expectedOutput = input.flatMap(typedSideDataMap.get)
    result.toList should contain theSameElementsAs expectedOutput
    val basePath = sparkeyUris.head.basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support .asTypedSparkeySideInput with a cache" in {
    val input = Seq("ab", "bc", "cd", "de")
    val typedSideData = Seq(("ab", Seq(1, 2)), ("bc", Seq(2, 3)), ("cd", Seq(3, 4)))
    val typedSideDataMap = typedSideData.toMap
    val cache = TestCache[String, Seq[Int]]()

    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey = sc.parallelize(typedSideData).mapValues(_.mkString(",")).asSparkey
      val si = sparkey.asTypedSparkeySideInput[Seq[Int]](cache) { b: Array[Byte] =>
        new String(b).split(",").map(_.toInt).toSeq
      }
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .flatMap((x, sic) => sic(si).get(x))
        .toSCollection
      (sparkey, result)
    }
    val expectedOutput = input.flatMap(typedSideDataMap.get)
    result should contain theSameElementsAs expectedOutput

    cache.underlying.stats().requestCount shouldBe input.size
    cache.underlying.stats().loadCount shouldBe input.toSet.size

    val basePath = sparkeyUris.head.basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support iteration with .asTypedSparkeySideInput" in {
    val input = Seq("1")
    val typedSideData = Seq(("ab", Seq(1, 2)), ("bc", Seq(2, 3)), ("cd", Seq(3, 4)))

    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey =
        sc.parallelize(typedSideData).mapValues(_.map(_.toString).mkString(",")).asSparkey
      val si = sparkey.asTypedSparkeySideInput[Seq[Int]] { b: Array[Byte] =>
        new String(b).split(",").toSeq.map(_.toInt)
      }
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .flatMap((_, sic) => sic(si).iterator.toList.sortBy(_._1).map(_._2))
        .toSCollection
      (sparkey, result)
    }
    val expectedOutput = typedSideData.map(_._2)
    result should contain theSameElementsAs expectedOutput

    val basePath = sparkeyUris.head.basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support iteration .asTypedSparkeySideInput with a cache" in {
    val input = Seq("1")
    val typedSideData = Seq(("ab", Seq(1, 2)), ("bc", Seq(2, 3)), ("cd", Seq(3, 4)))
    val cache = TestCache[String, Seq[Int]]()

    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey = sc.parallelize(typedSideData).mapValues(_.mkString(",")).asSparkey
      val si = sparkey.asTypedSparkeySideInput[Seq[Int]](cache) { b: Array[Byte] =>
        new String(b).split(",").map(_.toInt).toSeq
      }
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .flatMap((_, sic) => sic(si).iterator.toList.sortBy(_._1).map(_._2))
        .toSCollection
      (sparkey, result)
    }

    val expectedOutput = typedSideData.map(_._2)
    result should contain theSameElementsAs expectedOutput

    cache.underlying.stats().requestCount shouldBe typedSideData.size
    cache.underlying.stats().loadCount shouldBe 0

    val basePath = sparkeyUris.head.basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support iteration .asTypedSparkeySideInput with a cache and shards" in {
    val input = Seq("1")
    val typedSideData = Seq(("ab", Seq(1, 2)), ("bc", Seq(2, 3)), ("cd", Seq(3, 4)))
    val cache = TestCache[String, Seq[Int]]()

    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val sparkey =
        sc.parallelize(typedSideData).mapValues(_.mkString(",")).asSparkey(numShards = 2)
      val si = sparkey.asTypedSparkeySideInput[Seq[Int]](cache) { b: Array[Byte] =>
        new String(b).split(",").map(_.toInt).toSeq
      }
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .flatMap((_, sic) => sic(si).iterator.toList.sortBy(_._1).map(_._2))
        .toSCollection
      (sparkey, result)
    }

    val expectedOutput = typedSideData.map(_._2)
    result should contain theSameElementsAs expectedOutput

    cache.underlying.stats().requestCount shouldBe typedSideData.size
    cache.underlying.stats().loadCount shouldBe 0

    val basePath = sparkeyUris.head.basePath
    FileUtils.deleteDirectory(new File(basePath))
  }

  it should "support .asLargeMapSideInput" in {
    val input = Seq("ab", "bc", "cd", "de")
    val typedSideData = Seq(("ab", Seq(1, 2)), ("bc", Seq(2, 3)), ("cd", Seq(3, 4)))
    val typedSideDataMap = typedSideData.toMap

    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val si = sc.parallelize(typedSideData).asLargeMapSideInput
      val sparkey = sc.wrap(si.view.getPCollection).asInstanceOf[SCollection[SparkeyUri]]
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .flatMap((x, sic) => sic(si).get(x))
        .toSCollection
      (sparkey, result)
    }

    val expectedOutput = input.flatMap(typedSideDataMap.get)
    result should contain theSameElementsAs expectedOutput

    val basePath = sparkeyUris.head.basePath
    FileUtils.deleteDirectory(new File(basePath))
  }

  it should "support .asLargeMapSideInput with one shard" in {
    val input = Seq("ab", "bc", "cd", "de")
    val typedSideData = Seq(("ab", Seq(1, 2)), ("bc", Seq(2, 3)), ("cd", Seq(3, 4)))
    val typedSideDataMap = typedSideData.toMap

    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val si = sc.parallelize(typedSideData).asLargeMapSideInput(numShards = 1)
      val sparkey = sc.wrap(si.view.getPCollection).asInstanceOf[SCollection[SparkeyUri]]
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .flatMap((x, sic) => sic(si).get(x))
        .toSCollection
      (sparkey, result)
    }

    val expectedOutput = input.flatMap(typedSideDataMap.get)
    result should contain theSameElementsAs expectedOutput

    val basePath = sparkeyUris.head.basePath
    FileUtils.deleteDirectory(new File(basePath))
  }

  it should "support .asLargeSetSideInput" in {
    val input = Seq("ab", "bc", "cd", "de")
    val typedSideData = Set("ab", "bc", "cd")

    val (_, sparkeyUris, result) = runWithLocalOutput { sc =>
      val si = sc.parallelize(typedSideData).asLargeSetSideInput
      val sparkey = sc.wrap(si.view.getPCollection).asInstanceOf[SCollection[SparkeyUri]]
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .filter((x, sic) => sic(si).contains(x))
        .toSCollection
      (sparkey, result)
    }

    val expectedOutput = input.filter(typedSideData.contains)
    result should contain theSameElementsAs expectedOutput

    val basePath = sparkeyUris.head.basePath
    FileUtils.deleteDirectory(new File(basePath))
  }

  it should "support .asLargeSetSideInput with one shard" in {
    val input = Seq("ab", "bc", "cd", "de")
    val typedSideData = Set("ab", "bc", "cd")

    val (_, result, sparkeyUris) = runWithLocalOutput { sc =>
      val si = sc.parallelize(typedSideData).asLargeSetSideInput(numShards = 1)
      val sparkey = sc.wrap(si.view.getPCollection).asInstanceOf[SCollection[SparkeyUri]]
      val result = sc
        .parallelize(input)
        .withSideInputs(si)
        .filter((x, sic) => sic(si).contains(x))
        .toSCollection
      (result, sparkey)
    }

    val expectedOutput = input.filter(typedSideData.contains)
    result should contain theSameElementsAs expectedOutput

    val basePath = sparkeyUris.head.basePath
    FileUtils.deleteDirectory(new File(basePath))
  }

  it should "not override the regular hashJoin method" in {
    val lhsInput = Seq((1, "a"), (2, "c"), (3, "e"), (4, "g"))
    val rhsInput = Seq((1, "b"), (2, "d"), (3, "f"))
    val (_, result) = runWithLocalOutput { sc =>
      val rhs = sc.parallelize(rhsInput)
      val lhs = sc.parallelize(lhsInput)
      lhs.hashJoin(rhs)
    }

    val expectedOutput = List((1, ("a", "b")), (2, ("c", "d")), (3, ("e", "f")))
    result should contain theSameElementsAs expectedOutput
  }

}
