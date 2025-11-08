using Orleans.Storage;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Orleans.Investimentos.Silo.Serialization;

public class SystemTextJsonStorageSerializer : IGrainStorageSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        Converters = 
        {
            //new LegacyConverterFactory()
        }
    };

    public T Deserialize<T>(BinaryData input)
    {
        if (input.IsEmpty) return default!;

        var json = Encoding.UTF8.GetString(input.ToArray());

        // Fast path: if no Newtonsoft metadata, deserialize directly
        //if (!json.Contains("\"$type\"") && !json.Contains("\"$id\""))
        //{
        //    return JsonSerializer.Deserialize<T>(json, Options)!;
        //}

        //var cleaned = RemoveNewtonsoftMetadata(json);
        var cleaned = CleanNewtonsoftMeta(json);
        return JsonSerializer.Deserialize<T>(cleaned, Options)!;
    }

    public BinaryData Serialize<T>(T input)
    {
        var json = JsonSerializer.Serialize(input, Options);
        return BinaryData.FromString(json);
    }

    private static string CleanNewtonsoftMeta(string json)
    {
        var node = JsonNode.Parse(json)!;

        static void Clean(JsonNode? n)
        {
            if (n is JsonObject obj)
            {
                obj.Remove("$type");
                obj.Remove("$id");

                foreach (var kv in obj)
                    Clean(kv.Value);
            }
            else if (n is JsonArray arr)
            {
                foreach (var item in arr)
                    Clean(item);
            }
        }

        Clean(node);

        return node.ToJsonString();
    }

    private static void RemoveNewtonsoftMetadataV2(JsonNode? node)
    {
        if (node is JsonObject obj)
        {
            // Remove chaves $type, $id, $ref
            var keysToRemove = obj.Select(p => p.Key)
                                  .Where(k => k.StartsWith("$"))
                                  .ToList();

            foreach (var k in keysToRemove)
                obj.Remove(k);

            // Recursão nos filhos
            foreach (var kvp in obj)
                RemoveNewtonsoftMetadataV2(kvp.Value);
        }
        else if (node is JsonArray arr)
        {
            foreach (var item in arr)
                RemoveNewtonsoftMetadataV2(item);
        }
    }

    private static string RemoveNewtonsoftMetadata(string json)
    {
        using var doc = JsonDocument.Parse(json, new JsonDocumentOptions { AllowTrailingCommas = true });
        using var ms = new MemoryStream();
        using (var writer = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = false }))
        {
            WriteElementWithoutMetadata(doc.RootElement, writer);
        }
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    private static void WriteElementWithoutMetadata(JsonElement element, Utf8JsonWriter writer)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var prop in element.EnumerateObject())
                {
                    // skip Newtonsoft metadata properties
                    if (prop.NameEquals("$type") || prop.NameEquals("$id"))
                        continue;

                    writer.WritePropertyName(prop.Name);
                    WriteElementWithoutMetadata(prop.Value, writer);
                }
                writer.WriteEndObject();
                break;

            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in element.EnumerateArray())
                    WriteElementWithoutMetadata(item, writer);
                writer.WriteEndArray();
                break;

            case JsonValueKind.String:
                writer.WriteStringValue(element.GetString());
                break;

            case JsonValueKind.Number:
                if (element.TryGetInt64(out var l))
                    writer.WriteNumberValue(l);
                else if (element.TryGetDouble(out var d))
                    writer.WriteNumberValue(d);
                else
                    writer.WriteRawValue(element.GetRawText());
                break;

            case JsonValueKind.True:
                writer.WriteBooleanValue(true);
                break;

            case JsonValueKind.False:
                writer.WriteBooleanValue(false);
                break;

            case JsonValueKind.Null:
                writer.WriteNullValue();
                break;

            default:
                writer.WriteRawValue(element.GetRawText());
                break;
        }
    }

}
