package com.roundyuan.core

case class Info(name: String, website: String) extends Serializable {
  override def toString: String = name + "\t" + website
}
