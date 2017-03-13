package lesson3.settings

import lesson3.settings.Category.Category

class Settings(
                val ip: String,
                val category: Category,
                val value: Double,
                val period: Long)
  extends Serializable {
}


