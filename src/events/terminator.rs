use crate::events::segment::Segment;

// Terminator is responsible to close Segments that are
// either full or that the timer reached its limit
// It consumes the option from the Segment's collection
// so that the option is set to None ready to be initialized again
// when a new event for that index reaches intake.

pub(crate) struct Terminator(Option<Box<dyn crate::storage::Expeditor + Send>>);

pub(crate) fn new() -> Terminator {
    Terminator(None)
}

impl Terminator {
    pub(crate) fn terminate(&self, segment: Segment) {
        println!("Terminating the segment: {:?}", &segment);
    }
}
