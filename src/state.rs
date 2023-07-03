pub(super) enum State {
    Init,
    WaitingForOperator,
    Ready,
    Update,
}

impl State {
    pub(super) fn new() -> Self {
        Self::Init
    }

    pub(super) fn next(self) -> Self {
        match self {
            Self::Init => Self::WaitingForOperator,
            Self::WaitingForOperator => Self::Ready,
            Self::Ready => Self::Update,
            Self::Update => Self::Ready,
        }
    }
}