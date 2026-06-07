using Avro;
using Avro.Specific;

namespace AWSGsrSerDe.Tests
{
    public class User : ISpecificRecord
    {
        
        private static readonly Schema UserSchema = Schema.Parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"AWSGsrSerDe.Tests\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}");

        public string Name;
        public int FavoriteNumber;
        public string FavoriteColor;


        public object Get(int fieldPos)
        {
            return fieldPos switch
            {
                0 => Name,
                1 => FavoriteNumber,
                2 => FavoriteColor,
                _ => throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()")
            };
            ;
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: Name = (string)fieldValue; break;
                case 1: FavoriteNumber = (int)fieldValue; break;
                case 2: FavoriteColor = (string)fieldValue; break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            };
        }

        public Schema Schema => UserSchema;
    }
}