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

module.exports = {
  channels,
  priorities,
  topicsFor,
}
