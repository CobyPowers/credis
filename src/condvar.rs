/*
 * https://github.com/Amanieu/parking_lot/issues/165
 */

use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLockReadGuard, RwLockWriteGuard, WaitTimeoutResult};

#[derive(Debug, Default)]
pub struct CondvarRead {
    cv: Condvar,
    mtx: Mutex<()>,
}

impl CondvarRead {
    pub fn wait_for<T>(
        &self,
        g: &mut RwLockReadGuard<'_, T>,
        timeout: Duration,
    ) -> WaitTimeoutResult {
        let guard = self.mtx.lock();
        RwLockReadGuard::unlocked(g, || {
            let mut guard = guard;
            self.cv.wait_for(&mut guard, timeout)
        })
    }

    pub fn notify_one(&self) {
        let _guard = self.mtx.lock();
        self.cv.notify_one();
    }
}

#[derive(Debug, Default)]
pub struct CondvarWrite {
    cv: Condvar,
    mtx: Mutex<()>,
}

impl CondvarWrite {
    pub fn wait_for<T>(
        &self,
        g: &mut RwLockWriteGuard<'_, T>,
        timeout: Duration,
    ) -> WaitTimeoutResult {
        let guard = self.mtx.lock();
        RwLockWriteGuard::unlocked(g, || {
            let mut guard = guard;
            self.cv.wait_for(&mut guard, timeout)
        })
    }

    pub fn notify_one(&self) {
        let _guard = self.mtx.lock();
        self.cv.notify_one();
    }
}
