$version: "2"
namespace what.is.on.eire

use alloy#simpleRestJson

@simpleRestJson
service WhatIsOnEireApi {
    operations: [PullEvents]
}

@http(method: "GET", uri: "/event/pullEvents")
operation PullEvents {
    input: PullEventsInput
    output: PullEventsOutput
}

structure PullEventsInput {
    @httpHeader("irish-location")
    @required
    location: String
}

structure PullEventsOutput {
    @required
    events: IrishEventList
}
