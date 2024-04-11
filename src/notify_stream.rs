#![allow(clippy::mem_forget)]

use std::{
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures::{Future, Stream};
use tokio::sync::{futures::Notified, Notify};
use yoke::{Yoke, Yokeable};

/// A stream of notifications when a document is updated.
pub struct NotifyStream {
    notified: Option<Yoke<YokeableNotified<'static>, Arc<Notify>>>,
}

impl NotifyStream {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            notified: Some(Yoke::attach_to_cart(notify, |notify| {
                YokeableNotified(Box::pin(notify.notified()))
            })),
        }
    }
}

impl Stream for NotifyStream {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut res = Poll::Pending;

        let res_ref = &mut res;
        let notified = self.notified.take().expect("infallible");
        self.notified = Some(notified.map_project(move |mut notified, _| {
            *res_ref = notified.0.as_mut().poll(cx);
            notified
        }));

        ready!(res);

        let notify = self
            .notified
            .take()
            .expect("infallible")
            .into_backing_cart();
        self.notified = Some(Yoke::attach_to_cart(notify, |notify| {
            YokeableNotified(Box::pin(notify.notified()))
        }));

        Poll::Ready(Some(()))
    }
}

#[derive(Yokeable)]
struct YokeableNotified<'a>(Pin<Box<Notified<'a>>>);

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use futures::StreamExt;
    use tokio::sync::Notify;

    use crate::notify_stream::NotifyStream;

    #[tokio::test]
    async fn test() {
        let notify = Arc::new(Notify::new());
        notify.notify_waiters();
        notify.notify_waiters();

        let mut notify_stream = NotifyStream::new(notify.clone());
        notify.notify_waiters();
        notify.notify_waiters();

        assert_eq!(notify_stream.next().await, Some(()));
        assert!(
            tokio::time::timeout(Duration::from_millis(5), notify_stream.next())
                .await
                .is_err()
        );

        notify.notify_waiters();
        assert_eq!(notify_stream.next().await, Some(()));
    }
}
