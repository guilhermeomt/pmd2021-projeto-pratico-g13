var mapFunction = function () {
  if(this.category === "A") {
    emit(this.country, { count: 1 });
  }
};

var reduceFunction = function (key, values) {
  var value = { count: 0 };

  values.forEach((v) => {
      value.count += v.count;
  });

  return value;
};

db.universities.mapReduce(mapFunction, reduceFunction, {
  out: { inline: 1 },
  sort: { country: 1 },
});
