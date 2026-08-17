function eventType(envelope) {
  return envelope.type_name || envelope.typeName || null;
}
module.exports = { eventType, version: "0.1.0" };
