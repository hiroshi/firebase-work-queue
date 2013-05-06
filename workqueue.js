/**
 * This class manages a list of Firebase elements and dispatches items in it to 
 * be processed. It is designed to only process one item at a time. 
 *
 * It uses transactions to grab queue elements, so it's safe to run multiple
 * workers at the same time processing the same queue.
 *
 * @param queueRef A Firebase reference to the list of work items
 * @param processingCallback The callback to be called for each work item
 */
function WorkQueue(queueRef, processingCallback) {
    this.processingCallback = processingCallback;
    this.queue = queueRef.startAt().limit(1);
    this.callback = function(addedSnap) {
        var self = this;
        addedSnap.ref().transaction(function(val) {
            if (!val.started) {
                val.started = true;
                return val;
            }
        }, function(error, committed, snap) {
            if (error) throw error;
            if(committed) {
                console.log("Claimed a job.");
                self.queue.off("child_added", self.callback, self);
                snap.ref().remove();
                self.processingCallback(snap.val(), function() {
                    self.queue.on("child_added", self.callback, self);
                });
            } else {
                console.log("Another worker beat me to the job.");
            }
        });
    };
    this.queue.on("child_added", this.callback, this);
}

module.exports = WorkQueue;
