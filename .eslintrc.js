module.exports = {
  "extends": "kwebbl/lib/node",
  "env": {
    "jasmine": true
  },
  "rules": {
    "prefer-reflect": ["error", { "exceptions": ["getOwnPropertyNames"] }]
  }
};