var mapFunction = function () {
  emit({ country: this.country }, { count: 1 });
};

var reduceFunction = function (key, values) {
  var value = { count: 0 };

  values.forEach((v) => {
      value.count += v.count;
  });

  return value;
};

db.universities.mapReduce(mapFunction, reduceFunction, { out: { inline: 1 } });
