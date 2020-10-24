#r "System.Configuration"
#r "System.Data"
#r "Microsoft.IdentityModel.Tokens, Version=5.5.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35"
#r "Newtonsoft.Json"

using System.Text;
using System.Net;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using System.IdentityModel.Tokens.Jwt;
using Newtonsoft.Json.Linq;

// Constants
public static class Constants
{
    public const string DT_SIGNATURE_HEADER = "x-dt-signature";
    public const string DT_SIGNATURE_SECRET = "DT_SIGNATURE_SECRET";
}

public static string GetEnvironmentVariable(string name)
{
    return System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
}

public static string GetHeader(HttpRequestMessage req, string name)
{
    IEnumerable<string> headerValues;
    string header = null;

    if (req.Headers.TryGetValues(name, out headerValues))
    {
        header = headerValues.FirstOrDefault();
    }
    
    return header;
}

private static byte[] getBytes(string value) {
    return Encoding.UTF8.GetBytes(value);
}

private static string Base64UrlEncode(byte[] input) {
    var output = Convert.ToBase64String(input);
    output = output.Split('=')[0]; // Remove any trailing '='s
    output = output.Replace('+', '-'); // 62nd char of encoding
    output = output.Replace('/', '_'); // 63rd char of encoding
    return output;
}

public static bool VerifyJwtSignature(string jwt, ILogger log)
{
    string[] parts = jwt.Split(".".ToCharArray());
    var header = parts[0];
    var payload = parts[1];
    var signature = parts[2]; // Base64UrlEncoded

    byte[] bytesToSign = getBytes(string.Join(".", header, payload));

    byte[] secret = getBytes(GetEnvironmentVariable(Constants.DT_SIGNATURE_SECRET));

    var alg = new HMACSHA256(secret);
    var hash = alg.ComputeHash(bytesToSign);

    var computedSignature = Base64UrlEncode(hash);

    return computedSignature == signature;
}

public static bool JWTDecode(string token, ILogger log)
{
    var handler = new JwtSecurityTokenHandler();
    if (handler.CanReadToken(token) == false)
    {
        log.LogInformation("-- Could not read token.");
        return false;
    }

    // Validate token
    bool signature_status = VerifyJwtSignature(token, log);
    if (signature_status == false) {
        log.LogInformation("-- Bad signature.");
        return false;
    }
    return true;
}

static string Hash(string input)
{
    using (SHA1Managed sha1 = new SHA1Managed())
    {
        var hash = sha1.ComputeHash(Encoding.UTF8.GetBytes(input));
        var sb = new StringBuilder(hash.Length * 2);

        foreach (byte b in hash)
        {
            // can be "x2" if you want lowercase
            sb.Append(b.ToString("X2"));
        }

        return sb.ToString();
    }
}

public static bool VerifyChecksum(string jwt, string body_string, ILogger log)
{
    var handler = new JwtSecurityTokenHandler();

    string jsonToken = handler.ReadJwtToken(jwt).ToString();
    string payload = jsonToken.Split(".".ToCharArray())[1];

    dynamic data = JObject.Parse(payload);
    string payload_checksum = data.checksum;
    string calculated_checksum = Hash(body_string);

    return payload_checksum == calculated_checksum.ToString().ToLower();
}

public static bool ValidateSignature(HttpRequestMessage req, string body_string, ILogger log)
{
    // Verify existance of necessary environment variables
    if (GetEnvironmentVariable(Constants.DT_SIGNATURE_SECRET) == null)
    {
        log.LogInformation("ERROR: Missing secret.");
        return false;
    }

    // Verify existence of DT header
    if (GetHeader(req, Constants.DT_SIGNATURE_HEADER) == null)
    {
        log.LogInformation("ERROR: Missing header.");
        return false;
    }

    // verify secret against environment variables
    string token = GetHeader(req, Constants.DT_SIGNATURE_HEADER);
    bool status = JWTDecode(token, log);
    if (status == false)
    {
        log.LogInformation("ERROR: Bad signature.");
        return false;
    }

    // verify body checksum
    status = VerifyChecksum(token, body_string, log);
    if (status == false)
    {
        log.LogInformation("ERROR: Bad checksum.");
        return false;
    }

    // success
    log.LogInformation("Project Validated.");
    return true;
}

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, ILogger log)
{
    // Section A: Get event data
    string body_string = await req.Content.ReadAsStringAsync();
    JObject body = JObject.Parse(body_string);

    // Validate request content
    bool status = ValidateSignature(req, body_string, log);
    if (status == false)
    {
        return req.CreateResponse(HttpStatusCode.Unauthorized);
    }

        // Isolate some body content
    string eventId = (string)body["event"]["eventId"];
    string targetName = (string)body["event"]["targetName"];
    string deviceId = targetName.Substring(targetName.Length - 20);
    string timestamp = (string)body["event"]["timestamp"];
    string eventType = (string)body["event"]["eventType"];

    // Section B: Create the SQL query based on event type
    string text;
    string[] parameterNames;
    string[] parameterValues;

    switch (eventType) 
    {
        case "temperature":
        {
            text = "INSERT INTO temperature_events (event_id, device_id, timestamp, temperature) " +
                    "VALUES (@event_id, @device_id, @time, @temp);";
            string temperature = (string)body["event"]["data"]["temperature"]["value"];
            parameterNames = new string[] {"@event_id", "@device_id", "@time", "@temp"};
            parameterValues = new string[] {eventId, deviceId, timestamp, temperature.ToString()};
            break;
        }
         
        case "touch":
        {
            text = "INSERT INTO touch_events (event_id, device_id, timestamp) " +
                    "VALUES (@event_id, @device_id, @time);";
            parameterNames = new string[] {"@event_id", "@device_id", "@time"};
            parameterValues = new string[] {eventId, deviceId, timestamp};
            break;
        }
         
        case "objectPresent":
        {
            text = "INSERT INTO prox_events (event_id, device_id, timestamp, state) " +
                    "VALUES (@event_id, @device_id, @time, @state);";
            string state = (string)body["event"]["data"]["objectPresent"]["state"];
            parameterNames = new string[] {"@event_id", "@device_id", "@time", "@state"};
            parameterValues = new string[] {eventId, deviceId, timestamp, state};
            break;
        }
         
        case "waterPresent":
        {
            text = "INSERT INTO water_events (event_id, device_id, timestamp, state) " +
                    "VALUES (@event_id, @device_id, @time, @state);";
            string state = (string)body["event"]["data"]["waterPresent"]["state"];
            parameterNames = new string[] {"@event_id", "@device_id", "@time", "@state"};
            parameterValues = new string[] {eventId, deviceId, timestamp, state};
            break;
        }
         
        case "humidity":
        {
            text = "INSERT INTO humidity_events (event_id, device_id, timestamp, temperature, humidity) " +
                    "VALUES (@event_id, @device_id, @time, @temp, @humidity);";
            float temperature = (float)body["event"]["data"]["humidity"]["temperature"];
            float humidity = (float)body["event"]["data"]["humidity"]["relativeHumidity"];
            parameterNames = new string[] {"@event_id", "@device_id", "@time", "@temp", "@humidity"};
            parameterValues = new string[] {eventId, deviceId, timestamp, temperature.ToString(), humidity.ToString()};
            break;
        }
         
        case "objectPresentCount":
        {
            text = "INSERT INTO countingprox_events (event_id, device_id, timestamp, total) " +
                    "VALUES (@event_id, @device_id, @time, @total);";
            string total = (string)body["event"]["data"]["objectPresentCount"]["total"];
            parameterNames = new string[] {"@event_id", "@device_id", "@time", "@total"};
            parameterValues = new string[] {eventId, deviceId, timestamp, total.ToString()};
            break;
        }
         
        case "touchCount":
        {
            text = "INSERT INTO countingtouch_events (event_id, device_id, timestamp, total) " +
                    "VALUES (@event_id, @device_id, @time, @total);";
            string total = (string)body["event"]["data"]["touchCount"]["total"];
            parameterNames = new string[] {"@event_id", "@device_id", "@time", "@total"};
            parameterValues = new string[] {eventId, deviceId, timestamp, total.ToString()};
            break;
        }
         
        default:
            log.LogError("Unsupported event type received, check Data Connector. EventType = " + eventType);
            return req.CreateResponse(HttpStatusCode.OK); // Return OK to prevent data connector from resending
    }

    // Log for debugging and information
    log.LogInformation("Update: " + deviceId + ", " + eventType + ", " + timestamp);

    // Section C: Connect to SQL database and 
    var str = Environment.GetEnvironmentVariable("SQLDB_CONNECTION");
    using (SqlConnection conn = new SqlConnection(str))
    {
        conn.Open();
        using (SqlCommand cmd = new SqlCommand(text, conn))
        {
            // Add the parameters
            for (int i = 0; i < parameterValues.Length; i++)
            {
                cmd.Parameters.AddWithValue(parameterNames[i], parameterValues[i]);
            }

            try
            {
                // Execute query
                var rows = await cmd.ExecuteNonQueryAsync();
            }
            catch (SqlException ex) when (ex.Number == 2627)
            {
                // Ignore duplicate events...
            }
            catch (SqlException ex)
            {
                // Log the SQL exception for easier debugging
                for (int i = 0; i < ex.Errors.Count; i++) 
                {
                    log.LogInformation("SQL Exception: " + ex.Errors[i].Message);
                }
                 
                // Propagate other errors so that the Data Connector can retry later.
                return req.CreateResponse(HttpStatusCode.InternalServerError);
            }
        }
    }

    return req.CreateResponse(HttpStatusCode.OK);
}
