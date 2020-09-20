using System;
using System.Collections.Generic;
using System.Text;

namespace BigMission.CommandTools.Models
{
    public class Command
    {
        public string CommandType { get; set; }
        public string OriginId { get; set; }
        public string DestinationId { get; set; }
        public DateTime Timestamp { get; set; }
        public string Data { get; set; }
    }
}
