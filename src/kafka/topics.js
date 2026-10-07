const channels = {
  EMAIL: 'email',
  SMS: 'sms',
  PUSH: 'push',
}

const priorities = {
  LOW: 'low',
  NORMAL: 'normal',
  HIGH: 'high',
}

function topicsFor(channel, priority) {
  return `notifications.${channel}.${priority}`
}

// One retry "holding" topic per channel (not per priority - a failed
// message keeps whatever priority it already had, we just need somewhere
// to park it while it waits out its backoff before going back to the
// right priority topic).
function retryTopicFor(channel) {
  return `notifications.${channel}.retry`
}

module.exports = {
  channels,
  priorities,
  topicsFor,
  retryTopicFor,
}
