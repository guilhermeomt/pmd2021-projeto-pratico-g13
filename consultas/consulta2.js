var mapFunction = function () {
  emit(
    { university: this.university_name },
    {
      num_students: Number(this.num_students),
      ratio: Number(this.international_students),
    }
  );
};

var reduceFunction = function (key, values) {
  var value = 0;

  values.forEach((v) => {
    value = (v.num_students * v.ratio);
  });

  return value;
};

db.universities.mapReduce(mapFunction, reduceFunction, {
  out: "tmp_international_students",
})
db.tmp_international_students.find().sort({value: -1}).limit(10).pretty()