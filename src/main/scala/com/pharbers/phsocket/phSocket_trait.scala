package com.pharbers.phsocket

import java.net.Socket

trait phSocket_trait {
    val socket = new Socket("127.0.0.1", 9999)
}
