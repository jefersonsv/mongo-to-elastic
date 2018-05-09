using System;

namespace mongo_to_elastic
{
    public static class DateTimeHelper
    {
        /// <summary>
        /// https://stackoverflow.com/questions/249760/how-can-i-convert-a-unix-timestamp-to-datetime-and-vice-versa
        /// </summary>
        /// <param name="javaTimeStamp"></param>
        /// <returns></returns>
        public static DateTime JavaTimeStampToDateTime(double javaTimeStamp)
        {
            // Java timestamp is milliseconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddMilliseconds(javaTimeStamp).ToLocalTime();
            return dtDateTime;
        }

        public static DateTime UnixTimeStampToDateTime(string unixTimeStamp)
        {
            double timeStamp = Double.Parse(unixTimeStamp);
            // Unix timestamp is seconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds(timeStamp).ToLocalTime();
            return dtDateTime;
        }
    }
}