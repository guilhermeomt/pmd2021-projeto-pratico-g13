var mapFunction = function () {
  if (this.category === "A") {
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
  out: "tmp_country_ranking",
  sort: { country: 1 },
  
});
db.tmp_country_ranking.find().limit(1).pretty();
