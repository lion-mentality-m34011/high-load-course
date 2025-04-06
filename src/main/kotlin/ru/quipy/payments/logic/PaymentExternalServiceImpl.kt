package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import liquibase.repackaged.org.apache.commons.lang3.ObjectUtils.Null
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.File
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.timerTask
import kotlin.concurrent.write


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val responseTimes = Collections.synchronizedList(mutableListOf<Long>())

    private val timer = Timer()

//    init {
//        timer.scheduleAtFixedRate(timerTask {
//            saveToFile()
//        }, 0, 10000)
//    }

    private var client = OkHttpClient.Builder()
        .callTimeout(1100, TimeUnit.MILLISECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests, true)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        val maxRetries = 5
        val retryDelayMillis = 100L

        var attempt = 0
        var success = false

        while (attempt <= maxRetries && !success) {
            try {
                val start = System.nanoTime()
                client.newCall(request).execute().use { response ->
                    val duration = Duration.ofNanos(System.nanoTime() - start).toMillis()
                    responseTimes.add(duration)
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    success = true
                }
            } catch (e: Exception) {
                attempt++
                if (attempt >= maxRetries) {
                    throw e
                }
                logger.warn("[$accountName] Retry $attempt/$maxRetries for txId: $transactionId due to ${e.message}")
                Thread.sleep(retryDelayMillis)
            }
        }
        computeQuantiles(responseTimes)
    }

    private fun saveToFile() {
        synchronized(responseTimes) {
            val file = File("response_times.txt")
            file.writeText(responseTimes.joinToString(",") + "\n")
        }
    }

    fun computeQuantiles(responseTimes: List<Long>) {
        val snapshot = synchronized(responseTimes) { responseTimes.toList() }
        if (snapshot.isEmpty()) return

        val sorted = snapshot.sorted()
        fun quantile(p: Double): Long {
            val index = ((sorted.size - 1) * p).toInt()
            return sorted[index]
        }

        val quantileMap = mapOf(
            "p90" to quantile(0.90),
            "p95" to quantile(0.95),
        )
        println(quantileMap)
    }

//    fun calculatePercentiles(data: List<Long>, step: Double): List<Double> {
//        val sortedData = data.sorted()
//        val percentiles = mutableListOf<Double>()
//
//        for (i in 0..(1 / step).toInt()) {
//            val percentileIndex = (i * step * (sortedData.size - 1)).toInt()
//            percentiles.add(sortedData[percentileIndex].toDouble())
//        }
//
//        return percentiles
//    }
//
//    fun findPercentileDifferenceGreaterThan() {
//        val snapshot = synchronized(responseTimes) { responseTimes.toList() }
//        if (snapshot.isEmpty()) return
//        val percentiles = calculatePercentiles(snapshot, 0.05)
//
//        for (i in 1 until percentiles.size) {
//            val diff = percentiles[i] - percentiles[i - 1]
//            if (diff > 500) {
//                println("------------------------------------------------------------------------------------------" + percentiles[i - 1].toString())
//                clientLock.write {
//                    client = OkHttpClient.Builder()
//                        .callTimeout(percentiles[i - 1].toLong(), TimeUnit.MILLISECONDS)
//                        .build()
//                }
//                return
//            }
//        }
//
//        return
//    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()