package orderservice

import cats.Monad
import fs2.kafka.CommittableConsumerRecord

/** Single commit helper shared by every consumer, so the commit convention
  * (commit after handling, even for ignored/decoded-failure records) is defined
  * once.
  */
private[orderservice] object KafkaCommit {
  def commit[F[_]: Monad](
      committable: CommittableConsumerRecord[F, Option[String], Option[String]]
  ): F[Unit] = committable.offset.commit
}
