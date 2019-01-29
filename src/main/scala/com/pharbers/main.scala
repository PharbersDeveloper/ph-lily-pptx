package com.pharbers

import java.util.Date

import com.pharbers.phsocket.phSocketDriver
import com.pharbers.process.common.{phCommand, phLyFactory}
import com.pharbers.process.stm.step.pptx.slider.content.phReportContentTable

object main extends App {
    println(new Date())
    phReportContentTable.initTimeline("09 18")
    val jobid = phLyFactory.startProcess
    println("jobid:" + jobid)
    val socketDriver = phSocketDriver()
    socketDriver.createPPT(jobid)
    //    phLyFactory.setSaveMidDoc
    phLyFactory.getInstance("com.pharbers.process.flow.phBIFlowGenImpl").asInstanceOf[phCommand].exec(jobid)
    phLyFactory.endProcess
    println(new Date())
    println("jobid:" + jobid)
    Thread.sleep(5000)
}

//object test extends App {
//    val ppt: XMLSlideShow = new XMLSlideShow()
//    val slide = ppt.createSlide(ppt.getSlideMasters.get(0).getLayout(SlideLayout.TITLE_ONLY))
//    val title: XSLFTextShape= slide.getPlaceholder(0)
//    title.setAnchor(new Rectangle(100, 10,500,100))
//    val a: XSLFTextRun = title.setText("1231111111111111111111111111111111111111111111")
//    a.setFontSize(24.0)
//    val table = slide.createTable(10, 10)
//    table.setAnchor(new Rectangle(-20, 100, 0, 0))
////    table.getRows.get(0).setHeight(100)
//    table.setRowHeight(0,100)
//    table.setColumnWidth(0, 240)
//    table.setColumnWidth(1, 65)
//    table.getCell(0,0).setBorderColor(BorderEdge.right, Color.BLACK)
//    table.getCell(0,1).setBorderColor(BorderEdge.right, Color.BLACK)
//
//    ppt.write(new FileOutputStream("dcs.pptx"))
//}
//
object test2 extends App {
    val a = Array(1,2)
    val b = Array(-1,3)
    println(getValue(a,b))
    def getValue(nums1: Array[Int], nums2: Array[Int]): Double = {
        val length = nums1.length + nums2.length
        var  resultIndex: List[Int] = Nil
        var result: List[Int] = Nil
        if (length % 2 == 1) {
            resultIndex = List(length / 2)
        } else {
            resultIndex = List(length / 2, length / 2 - 1)
        }
        val ints1 = nums1
        val ints2 = nums2
        var index1 = ints1.length / 2
        var index2 = ints2.length / 2
        while (resultIndex.nonEmpty) {
            val nums1InNums2Index = getIndexInNum2(ints1(index1), ints2, ints2.length / 2)
            val nums2InNums1Index = getIndexInNum1(ints2(index2), ints1, ints1.length / 2)
            if (resultIndex.contains(nums1InNums2Index + index1)) {
                result = result :+ ints1(index1)
                resultIndex = resultIndex.filter(x => x != nums1InNums2Index + index1)
            }
            if (resultIndex.contains(nums2InNums1Index + index2)) {
                result = result :+ ints2(index2)
                resultIndex = resultIndex.filter(x => x != nums2InNums1Index + index2)
            }
            index1 = (nums2InNums1Index + index1) / 2
            index2 = (index2 + nums1InNums2Index) / 2
        }
        result.sum.toDouble / result.length
    }

    def getOneNumsValue(nums: Array[Int]): Double = {
        val length = nums.length
        var  resultIndex: List[Int] = Nil
        var result: List[Int] = Nil
        if (length % 2 == 1) {
            resultIndex = List(length / 2)
        } else {
            resultIndex = List(length / 2, length / 2 - 1)
        }
        result = resultIndex.map(x => nums(x))
        result.sum.toDouble / result.length
    }

    def getIndexInNum2(num: Int, nums: Array[Int], index: Int): Int = {
        if (index > nums.length - 1) return nums.length

        if (nums(index) < num && (index == nums.length - 1 || num <= nums(index + 1))) {
            return index + 1
        }
        if (index <= 0) return 0
        if (nums(index) <= num) {
            getIndexInNum2(num, nums, (index + nums.length) / 2)
        } else {
            getIndexInNum2(num, nums, (-1 + index) / 2)
        }

    }

    def getIndexInNum1(num: Int, nums: Array[Int], index: Int): Int = {
        if (index > nums.length - 1) return nums.length

        if (nums(index) <= num && (index == nums.length - 1 || num < nums(index + 1))) {
            return index + 1
        }
        if (index <= 0) return 0
        if (nums(index) <= num) {
            getIndexInNum2(num, nums, (index + nums.length) / 2)
        } else {
            getIndexInNum2(num, nums, (-1 + index) / 2)
        }

    }
}
