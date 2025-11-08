using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Orleans.Investimentos.Silo.Serialization;

public class LegacyConverter<T> : JsonConverter<T>
{
    public override T? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        // Lê o JSON como JsonDocument
        var node = JsonNode.Parse(ref reader);
        if (node is null)
            return default;

        bool isNewtonsoftLegacy = Attribute.IsDefined(typeToConvert, typeof(JsonNewtonsoftLegacyAttribute));

        var processed = ProcessNode(node, typeToConvert, isNewtonsoftLegacy);

        var cleanOptions = new JsonSerializerOptions(options);
        for (int i = cleanOptions.Converters.Count - 1; i >= 0; i--)
        {
            var convType = cleanOptions.Converters[i].GetType();
            if (convType.IsGenericType && convType.GetGenericTypeDefinition() == typeof(LegacyConverter<>)
                || convType == typeof(LegacyConverterFactory))
            {
                cleanOptions.Converters.RemoveAt(i);
            }
        }

        return processed.Deserialize<T>(cleanOptions);        
    }

    public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        => JsonSerializer.Serialize(writer, value, options);

    private static JsonNode ProcessNode(JsonNode node, Type? targetType, bool isNewtonsoftLegacy)
    {
        if (node is not JsonObject obj)
            return node;        

        // Mapeia propriedades renomeadas
        var legacyMap = targetType?.GetProperties()
            .SelectMany(p => p.GetCustomAttributes<JsonPropertyNamesLegacyAttribute>()
                .SelectMany(a => a.LegacyNames.Select(n => (Old: n, New: p.Name))))
            .ToDictionary(x => x.Old, x => x.New, StringComparer.OrdinalIgnoreCase) ?? [];
                
        var toRemove = new List<string>();
        var toAdd = new Dictionary<string, JsonNode?>();

        foreach (var kvp in obj)
        {
            string propName = kvp.Key;
            JsonNode? value = kvp.Value;

            // Remove metadados Newtonsoft
            if (isNewtonsoftLegacy && propName.StartsWith("$"))
            {
                toRemove.Add(propName);
                continue;
            }

            // Renomeia propriedades legadas
            if (legacyMap.TryGetValue(propName, out var newName))
            {
                toRemove.Add(propName);
                propName = newName;
            }

            // Se tiver subobjeto, processa recursivamente
            if (value is JsonObject child)
            {
                var propInfo = GetPropertyInfo(targetType, propName)!;            
                value = ProcessNode(child, propInfo?.PropertyType, isNewtonsoftLegacy);
            }

            toAdd[propName] = value;
        }

        foreach (var r in toRemove)
            obj.Remove(r);

        foreach (var kv in toAdd)
            obj[kv.Key] = kv.Value;

        return obj;
    }

    private static PropertyInfo? GetPropertyInfo(Type? targetType, string propName)
    {
        if (targetType == null)
            return null;

        var candidates = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => string.Equals(p.Name, propName, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        if (candidates.Length == 0)
            return null;

        if (candidates.Length == 1)
            return candidates[0];

        // Tenta escolher a propriedade declarada no tipo alvo (aquela que usa `new` em classes derivadas).
        var declaredOnTarget = candidates.FirstOrDefault(p => p.DeclaringType == targetType);
        if (declaredOnTarget != null)
            return declaredOnTarget;

        // Se nenhuma for declarada no tipo alvo, retorna a primeira encontrada (fallback).
        return candidates[0];
    }
}


